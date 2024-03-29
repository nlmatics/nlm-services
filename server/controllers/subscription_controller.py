import logging
import os
import traceback
from datetime import date

import stripe
from flask import jsonify
from flask import make_response
from flask import redirect
from flask import request
from nlm_utils.utils.utils import ensure_bool

from server import unauthorized_response
from server.storage import nosql_db


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BACKEND_URL = os.getenv("BACKEND_URL")
FRONTEND_URL = os.getenv("FRONTEND_URL")
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY", "")
STRIPE_WEBHOOK_ENDPOINT_SECRET = os.getenv(
    "STRIPE_WEBHOOK_ENDPOINT_SECRET"
)
DEFAULT_SUBSCRIPTION_TRIAL_DAYS = os.getenv("DEFAULT_SUBSCRIPTION_TRIAL_DAYS", 7)
DEFAULT_SUBSCRIPTION_PLAN = os.getenv("DEFAULT_SUBSCRIPTION_PLAN", "BASIC")
PAYMENT_CONTROLLED_RENEWABLE_RESOURCES = ensure_bool(
    os.getenv("PAYMENT_CONTROLLED_RENEWABLE_RESOURCES", False),
)

# This is your test secret API key.
stripe.api_key = STRIPE_API_KEY


def get_plans(
    user,
    token_info,
):
    try:
        user_obj = token_info["user_obj"]
        if user_obj["email_id"] != user:
            logger.error(f"user {user} not authorized to request plans for other users")
            return unauthorized_response()

        plans = []
        # get the currently selected plan if any
        current_plans = []
        if not user_obj.get("stripe_conf", {}).get("stripe_customer_id", ""):
            # Check whether there is a Stripe Customer created with the email address.
            response = stripe.Customer.search(query=f"email:'{user}'")
            stripe_customer_id = None
            if not response.get("data", []):
                app_name = user_obj["app_name"]
                if app_name:
                    app_settings = nosql_db.get_application_settings(app_name)
                    if app_settings.get("users", {}).get(
                        "create_stripe_customer",
                        False,
                    ):
                        metadata = {
                            app_name: True,
                        }
                        stripe_customer_id = create_stripe_customer(
                            user,
                            user_name=user_obj["first_name"]
                            + " "
                            + user_obj["last_name"],
                            metadata=metadata,
                        )
            else:
                stripe_customer_id = response["data"][0]["id"]
                app_name = user_obj["app_name"]
                if app_name:
                    metadata = {
                        app_name: True,
                    }
                    stripe.Customer.modify(
                        stripe_customer_id,
                        metadata=metadata,
                    )
            if stripe_customer_id:
                nosql_db.update_user(
                    {
                        "stripe_conf": {
                            "stripe_customer_id": stripe_customer_id,
                        },
                    },
                    user_id=user_obj["id"],
                    email=user,
                )
                logger.info(
                    f"Stripe customer ID created for {user}",
                )

        stripe_subscriptions = user_obj.get("stripe_conf", {}).get("subscriptions", [])
        for subs in stripe_subscriptions:
            subscription_id = subs.get("stripe_subscription_id", "")
            if subscription_id:
                current_plan = {}
                current_subscription = stripe.Subscription.retrieve(
                    subscription_id,
                    expand=["customer"],
                )
                selected_plan_price_id = current_subscription["plan"]["id"]
                current_plan["price_id"] = selected_plan_price_id
                end_date_str = date.fromtimestamp(
                    current_subscription["current_period_end"],
                ).strftime("%B %d, %Y")

                if current_subscription["cancel_at_period_end"]:
                    current_plan["status"] = "canceled"
                    current_plan["status_message"] = (
                        f"We're sorry to see you go.\n"
                        f"Your subscription has been canceled and will expire on {end_date_str}."
                    )
                elif current_subscription["canceled_at"]:
                    current_plan["status"] = "canceled"
                    canceled_at_str = date.fromtimestamp(
                        current_subscription["canceled_at"],
                    ).strftime("%B %d, %Y")
                    current_plan["status_message"] = (
                        f"We're sorry to see you go.\n"
                        f"Your subscription has been canceled and will expire on {canceled_at_str}."
                    )
                elif current_subscription["status"] == "trialing":
                    end_date_str = date.fromtimestamp(
                        current_subscription["trial_end"],
                    ).strftime("%B %d, %Y")
                    if (
                        not current_subscription["default_payment_method"]
                        and not current_subscription.default_source
                        and not current_subscription.customer.default_source
                        and not current_subscription.customer.invoice_settings.default_payment_method
                    ):
                        current_plan["status"] = "trial_period"
                        current_plan["status_message"] = (
                            f"Your trial period will expire on {end_date_str}.\n"
                            f"Upgrade now to continue using our service."
                        )
                    else:
                        current_plan["status"] = "active"
                        current_plan[
                            "status_message"
                        ] = "Thank you for subscribing! Your subscription is now active and ready to use."
                elif current_subscription["status"] == "past_due":
                    current_plan["status"] = "past_due"
                    current_plan["status_message"] = (
                        "Your subscription is past due.\n"
                        "Please complete the payment process to resume using our service."
                    )
                else:
                    action_taken = False
                    if (
                        current_subscription["status"] == "active"
                        and not current_subscription["default_payment_method"]
                        and not current_subscription["default_source"]
                    ):
                        stripe_customer_id = user_obj.get("stripe_conf", {}).get(
                            "stripe_customer_id",
                            "",
                        )
                        stripe_customer = None
                        if stripe_customer_id:
                            stripe_customer = stripe.Customer.retrieve(
                                stripe_customer_id,
                            )
                        if (
                            stripe_customer
                            and not stripe_customer.get("invoice_settings", {}).get(
                                "default_payment_method",
                                "",
                            )
                            and not stripe_customer.get("default_source", "")
                        ):
                            # No payment settings in subscription or in Customer. Ask for the details.
                            end_date_str = date.fromtimestamp(
                                current_subscription["trial_end"],
                            ).strftime("%B %d, %Y")
                            # If there is no payment information. We cannot charge the customer yet.
                            # Update the status to reflect that.
                            current_plan[
                                "status"
                            ] = "trialing_pending_payment_information"
                            current_plan["status_message"] = (
                                "Your subscription is pending payment information.\n"
                                "Please update your billing information to avoid service interruption."
                            )
                            action_taken = True
                    if not action_taken:
                        current_plan["status"] = "active"
                        current_plan["status_message"] = (
                            "Thank you for subscribing!\n"
                            "Your subscription is now active and ready to use."
                        )
                current_plans.append(current_plan)

        for subscription in nosql_db.db["nlm_subscriptions"].find(
            {"subs_type": "nlmatics_paid_plan"},
        ):
            # get prices
            prices = stripe.Price.list(
                active=True,
                product=subscription["stripe_product_id"],
            )["data"]
            product_prices = []
            for price in prices:
                interval = price["recurring"]["interval"]
                if interval == "month":
                    interval = "Monthly"
                elif interval == "year":
                    interval = "Yearly"
                unit_amount = price["unit_amount"] / 100
                product_price = {
                    "id": price["id"],
                    "interval": interval,
                    "unit_amount": unit_amount,
                }
                nlm_resource = subscription.get("nlm_resource", "")
                if nlm_resource:
                    product_price[f"{nlm_resource}"] = {
                        "id": subscription.get("nlm_resource_id", ""),
                    }
                for current_plan in current_plans:
                    if price["id"] == current_plan["price_id"]:
                        product_price["current_plan"] = current_plan
                product_prices.append(product_price)

            # get features
            plan = {
                "name": subscription["display_name"],
                "price_options": product_prices,
                "features": [],
            }

            for feature in nosql_db.db["nlm_features"].find(
                {"key": {"$in": subscription["included_features"]}},
                {"_id": 0},
            ):
                plan["features"].append(feature["description"])

            plans.append(plan)
        return plans
    except Exception as e:
        logger.error(
            f"error fetching document, err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": "Server error"}), 500)


def change_plan(user, token_info, body):
    try:
        user_obj = token_info["user_obj"]
        # Get the users subscriptions
        subscriptions = user_obj.get("stripe_conf", {}).get("subscriptions", [])
        found_subscription = False
        subscription = None
        for subs in subscriptions:
            subs_prod_plan_id = subs.get("stripe_product_plan_id", "")
            if subs_prod_plan_id and subs_prod_plan_id == body["lookup_key"]["id"]:
                subscription_id = subs.get("stripe_subscription_id", "")
                if subscription_id:
                    subscription = stripe.Subscription.retrieve(
                        subscription_id,
                        expand=["customer"],
                    )
                    if subscription:
                        if subscription.canceled_at:
                            found_subscription = False
                            break
                        elif (
                            (
                                not subscription.default_payment_method
                                and not subscription.default_source
                                and not subscription.customer.default_source
                                and not subscription.customer.invoice_settings.default_payment_method
                            )
                            or subscription.status == "trialing"
                            or subscription.status == "past_due"
                        ):
                            found_subscription = True
                            break
        if subscription and body["lookup_key"].get("update_payment_info", False):
            return create_setup_checkout_session(
                token_info,
                subscription.id,
                subscription.status,
                body,
            )
        if not found_subscription:
            if not subscription:
                return create_subscription_checkout_session(user, token_info, body)
            elif subscription.canceled_at:
                # Update the subscription to not cancel.
                subs_response = stripe.Subscription.modify(
                    subscription.id,
                    cancel_at_period_end=False,
                )
                active_subscriptions = 0
                for subs in subscriptions:
                    if subs["stripe_subscription_id"] == subscription.id:
                        if subs_response["status"] == "trialing":
                            subs["status"] = "trialing_active"
                        else:
                            subs["status"] = "active"
                    if subs["status"] in ["active", "trialing_active"]:
                        active_subscriptions += 1
                set_data = {
                    "stripe_conf.subscriptions": subscriptions,
                }
                if active_subscriptions >= 1:
                    # Upgrade the metered subscription.
                    app_name = user_obj["app_name"]
                    if app_name:
                        app_settings = nosql_db.get_application_settings(app_name)
                        set_data["subscription_plan"] = app_settings.get(
                            "users",
                            {},
                        ).get(
                            "default_paid_metered_plan",
                            DEFAULT_SUBSCRIPTION_PLAN,
                        )
                nosql_db.update_user(set_data, user_id=user_obj["id"])
                return get_plans(user, token_info)
        else:
            return create_setup_checkout_session(
                token_info,
                subscription.id,
                subscription.status,
                body,
            )

    except Exception as e:
        logger.error(
            f"error fetching document, err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": "Server error"}), 500)


def cancel_plan(user, token_info, body):
    try:
        user_obj = token_info["user_obj"]
        # Get the users subscriptions
        subscriptions = user_obj.get("stripe_conf", {}).get("subscriptions", [])
        found_subscription = False
        subscription_id = None
        for subs in subscriptions:
            subs_prod_plan_id = subs.get("stripe_product_plan_id", "")
            if subs_prod_plan_id and subs_prod_plan_id == body["lookup_key"]["id"]:
                subscription_id = subs.get("stripe_subscription_id", "")
                if subscription_id:
                    found_subscription = True
                    break
        if not found_subscription:
            logger.error(
                f"Subscription not found with {body['lookup_key']['id']} for user {user_obj['email_id']}",
            )
            return make_response(
                jsonify({"status": "fail", "reason": "Server error"}),
                500,
            )
        else:
            subs_response = stripe.Subscription.modify(
                subscription_id,
                cancel_at_period_end=True,
            )
            if subs_response and subs_response["cancel_at_period_end"]:
                for subs in subscriptions:
                    if subs["stripe_product_plan_id"] == body["lookup_key"]["id"]:
                        if subs_response["status"] == "trialing":
                            subs["status"] = "trialing"
                            subs["end_date"] = date.fromtimestamp(
                                subs_response["trial_end"],
                            ).strftime("%Y-%m-%d")
                        else:
                            subs["status"] = "canceled"
                set_data = {
                    "stripe_conf.subscriptions": subscriptions,
                }

                nosql_db.update_user(set_data, user_id=user_obj["id"])
                return get_plans(user, token_info)
            else:
                logger.error(
                    f"Error in deleting subscription {body['lookup_key']['id']} for user {user_obj['email_id']}",
                )
                return make_response(
                    jsonify({"status": "fail", "reason": "Server error"}),
                    500,
                )
    except Exception as e:
        logger.error(
            f"error fetching document, err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": "Server error"}), 500)


def create_subscription_checkout_session(
    user,
    token_info,
    body,
):

    try:
        user_obj = token_info["user_obj"]
        if not user_obj:
            logger.error(f"invalid user {user}, action denied")
            return unauthorized_response()
        user_id = user_obj["id"]
        logger.info(
            f"creating subscription checkout session for user: {user_id} .. {body['lookup_key']}",
        )
        metadata = {
            "app_name": user_obj["app_name"],
            "nlm_user_id": user_id,
            "stripe_product_plan_id": body["lookup_key"]["id"],
            "return_path": body["lookup_key"].get("return_path", "/plan-and-usage"),
        }
        resource_id = body["lookup_key"].get("workspace", {}).get("id", "")
        if resource_id:
            metadata["resource"] = "workspace"
            metadata["resource_id"] = resource_id
        else:
            resource_id = body["lookup_key"].get("subscriptions", {}).get("id", "")
            if resource_id:
                metadata["resource"] = "subscriptions"
                metadata["resource_id"] = resource_id

        checkout_session = stripe.checkout.Session.create(
            line_items=[
                {
                    "price": body["lookup_key"]["id"],
                    "quantity": 1,
                },
            ],
            mode="subscription",
            customer=user_obj.get("stripe_conf", {}).get("stripe_customer_id", None),
            success_url=BACKEND_URL
            + "/api/subscription/handleCheckoutSuccess?checkout_id={CHECKOUT_SESSION_ID}",
            cancel_url=BACKEND_URL
            + "/api/subscription/handleCheckoutCancel?checkout_id={CHECKOUT_SESSION_ID}",
            metadata=metadata,
            subscription_data={
                "metadata": metadata,
            },
        )
        nosql_db.add_subscription_session(
            user_id,
            checkout_session.id,
            body["lookup_key"],
        )
        return make_response(
            jsonify({"redirect_url": checkout_session.url}),
            200,
        )
    except Exception as e:
        logger.error(
            f"error fetching document, err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": "Server error"}), 500)


def create_setup_checkout_session(
    token_info,
    stripe_subscription_id,
    subscription_status,
    body,
):

    try:
        user_obj = token_info["user_obj"]
        stripe_customer_id = user_obj.get("stripe_conf", {}).get(
            "stripe_customer_id",
            None,
        )
        logger.info(
            f"creating payment setup checkout session for user: {user_obj['email_id']}",
        )

        checkout_session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="setup",
            customer=stripe_customer_id,
            success_url=BACKEND_URL
            + "/api/subscription/handleCheckoutSuccess?checkout_id={CHECKOUT_SESSION_ID}",
            cancel_url=BACKEND_URL
            + "/api/subscription/handleCheckoutCancel?checkout_id={CHECKOUT_SESSION_ID}",
            setup_intent_data={
                "metadata": {
                    "nlm_user_id": user_obj["id"],
                    "app_name": user_obj["app_name"],
                    "subscription_id": stripe_subscription_id,
                    "subscription_status": subscription_status,
                    "return_path": body["lookup_key"].get(
                        "return_path",
                        "/plan-and-usage",
                    ),
                },
            },
            metadata={
                "nlm_user_id": user_obj["id"],
                "app_name": user_obj["app_name"],
                "return_path": body["lookup_key"].get(
                    "return_path",
                    "/plan-and-usage",
                ),
            },
        )

        return make_response(
            jsonify({"redirect_url": checkout_session.url}),
            200,
        )
    except Exception as e:
        logger.error(
            f"error fetching document, err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": "Server error"}), 500)


def handle_checkout_success(checkout_id):
    logger.info(f"stripe session success: {checkout_id}")
    checkout_session_id = checkout_id
    checkout_session = stripe.checkout.Session.retrieve(checkout_session_id)
    return_path = checkout_session["metadata"].get("return_path", "/plan-and-usage")
    # Retrieve the user.
    if (
        checkout_session["mode"] == "subscription"
        and checkout_session["status"] == "complete"
        and checkout_session["payment_status"] == "paid"
    ):
        nlm_user_id = checkout_session["metadata"].get("nlm_user_id", None)
        app_name = checkout_session["metadata"].get("app_name", "")
        stripe_product_plan_id = checkout_session["metadata"].get(
            "stripe_product_plan_id",
            "",
        )
        if nlm_user_id:
            user = nosql_db.get_user(nlm_user_id, include_stripe_conf=True)
            # Update the new subscription.
            if user.stripe_conf:
                session_metadata = checkout_session["metadata"]
                resource = session_metadata.get("resource", "")
                resource_id = session_metadata.get("resource_id", "")

                subscribed_workspaces = user.subscribed_workspaces
                restricted_workspaces = user.restricted_workspaces or []
                if resource_id and resource == "workspace":
                    if resource_id not in subscribed_workspaces:
                        subscribed_workspaces.append(resource_id)
                    if resource_id in restricted_workspaces:
                        restricted_workspaces.remove(resource_id)

                subscriptions = user.stripe_conf.get("subscriptions", [])
                found_subscription = False
                for subs in subscriptions:
                    if subs["stripe_product_plan_id"] == stripe_product_plan_id:
                        found_subscription = True
                        subs["status"] = "active"
                        if resource and not subs.get("stripe_resource", ""):
                            subs["stripe_resource"] = resource
                        if not subs.get("stripe_subscription_id", ""):
                            subs["stripe_subscription_id"] = checkout_session[
                                "subscription"
                            ]
                        if not subs.get("stripe_resource_id", ""):
                            subs["stripe_resource_id"] = resource_id
                        # Retrieve the subscription and update the end_date
                        current_subscription = stripe.Subscription.retrieve(
                            subs["stripe_subscription_id"],
                        )
                        subs["end_date"] = date.fromtimestamp(
                            current_subscription["current_period_end"],
                        ).strftime("%Y-%m-%d")
                        break

                if not found_subscription:
                    # Retrieve the subscription and update the end_date
                    current_subscription = stripe.Subscription.retrieve(
                        checkout_session["subscription"],
                    )
                    subscriptions.append(
                        {
                            "stripe_product_plan_id": stripe_product_plan_id,
                            "stripe_subscription_id": checkout_session["subscription"],
                            "status": "active",
                            "stripe_resource": resource,
                            "stripe_resource_id": resource_id,
                            "end_date": date.fromtimestamp(
                                current_subscription["current_period_end"],
                            ).strftime("%Y-%m-%d"),
                        },
                    )
                active_subscriptions = 0
                for subs in subscriptions:
                    if subs["status"] in ["active", "trialing_active"]:
                        active_subscriptions += 1
                subscription_plan = None
                if active_subscriptions >= 1:
                    # Upgrade the metered subscription.
                    if app_name:
                        app_settings = nosql_db.get_application_settings(app_name)
                        subscription_plan = app_settings.get("users", {}).get(
                            "default_paid_metered_plan",
                            DEFAULT_SUBSCRIPTION_PLAN,
                        )
                        if PAYMENT_CONTROLLED_RENEWABLE_RESOURCES:
                            nosql_db.reset_renewable_resources(
                                user.id,
                                subscription_plan,
                            )

                nosql_db.set_subscription_session_status(
                    checkout_id,
                    "success",
                    subscriptions,
                    subscribed_workspaces,
                    restricted_workspaces,
                    subscription_plan,
                )
                # existing_subs_id = user.stripe_conf.get("stripe_subscription_id", "")
                # stripe.Subscription.modify(
                #     existing_subs_id,
                #     pause_collection={
                #         "behavior": "mark_uncollectible",
                #     },
                # )
    elif (
        checkout_session["mode"] == "setup" and checkout_session["status"] == "complete"
    ):
        # Handle the payment setup process.
        setup_intent_id = checkout_session["setup_intent"]
        nlm_user_id = checkout_session["metadata"].get("nlm_user_id", None)
        app_name = checkout_session["metadata"].get("app_name", "")
        if setup_intent_id and nlm_user_id:
            setup_intent = stripe.SetupIntent.retrieve(setup_intent_id)
            user = nosql_db.get_user(nlm_user_id, include_stripe_conf=True)
            if user.stripe_conf:
                stripe_customer_id = user.stripe_conf.get("stripe_customer_id", "")
                if stripe_customer_id == setup_intent["customer"]:
                    # We are dealing with the same customer here. Extra caution
                    subscription_id = setup_intent["metadata"].get(
                        "subscription_id",
                        "",
                    )
                    subscription_status = setup_intent["metadata"].get(
                        "subscription_status",
                        "",
                    )
                    payment_method = setup_intent["payment_method"]
                    # Update the default payment method for the Customer.
                    stripe_customer = stripe.Customer.retrieve(stripe_customer_id)
                    if (
                        not stripe_customer.get("invoice_settings", {}).get(
                            "default_payment_method",
                            "",
                        )
                        or subscription_status == "past_due"
                    ):
                        stripe.Customer.modify(
                            stripe_customer_id,
                            invoice_settings={"default_payment_method": payment_method},
                        )
                    # Update the default payment method on the Subscription.
                    stripe.Subscription.modify(
                        subscription_id,
                        default_payment_method=payment_method,
                        cancel_at_period_end=False,
                    )
                    # Update the subscription status
                    subscriptions = user.stripe_conf.get("subscriptions", [])
                    for subs in subscriptions:
                        if subs["stripe_subscription_id"] == subscription_id:
                            if subs["status"] in ["trialing", "trialing_active"]:
                                subs["status"] = "trialing_active"
                            else:
                                subs["status"] = "active"
                            break
                    set_data = {
                        "stripe_conf.subscriptions": subscriptions,
                    }

                    active_subscriptions = 0
                    for subs in subscriptions:
                        if subs["status"] in ["active", "trialing_active"]:
                            active_subscriptions += 1
                    if active_subscriptions >= 1:
                        # Upgrade the metered subscription.
                        if app_name:
                            app_settings = nosql_db.get_application_settings(app_name)
                            set_data["subscription_plan"] = app_settings.get(
                                "users",
                                {},
                            ).get(
                                "default_paid_metered_plan",
                                DEFAULT_SUBSCRIPTION_PLAN,
                            )
                            if PAYMENT_CONTROLLED_RENEWABLE_RESOURCES:
                                nosql_db.reset_renewable_resources(
                                    user.id,
                                    set_data["subscription_plan"],
                                )

                    nosql_db.update_user(set_data, user_id=nlm_user_id)

    # This is the URL to which the customer will be redirected after they are
    # done managing their billing with the portal.
    return_url = FRONTEND_URL + return_path
    portal_session = stripe.billing_portal.Session.create(
        customer=checkout_session.customer,
        return_url=return_url,
    )
    return redirect(portal_session.url, code=303)


def handle_checkout_cancel(checkout_id):
    logger.info(f"stripe session cancel: {checkout_id}")
    checkout_session = stripe.checkout.Session.retrieve(checkout_id)
    return_path = checkout_session["metadata"].get("return_path", "/plan-and-usage")
    return_url = FRONTEND_URL + return_path
    nosql_db.set_subscription_session_status(checkout_id, "cancel")

    return redirect(return_url, code=303)


def create_stripe_customer_and_subscription(
    customer_email_id,
    product_price_plan_id,
    user_name="",
    app_name="",
):
    # Create the Stripe customer.
    metadata = {}
    if app_name:
        metadata["app_name"] = app_name

    try:
        customer = stripe.Customer.create(
            description=user_name.strip(),
            email=customer_email_id,
            metadata=metadata,
        )
        customer_id = customer["id"]
        subscription_id = create_stripe_subscription(
            customer_id,
            product_price_plan_id,
            7,
        )

    except Exception as e:
        logger.error(
            f"error creating stripe customer / subscription, err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        customer_id = None
        subscription_id = None
    return customer_id, subscription_id


def create_stripe_customer(
    customer_email_id,
    user_name="",
    app_name="",
    metadata=None,
):
    # Create the Stripe customer.
    if not metadata:
        metadata = {}
    if app_name:
        metadata["app_name"] = app_name

    try:
        customer = stripe.Customer.create(
            description=user_name.strip(),
            email=customer_email_id,
            metadata=metadata,
        )
        customer_id = customer["id"]

    except Exception as e:
        logger.error(
            f"Error creating stripe customer err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        customer_id = None
    return customer_id


def create_stripe_subscription(
    stripe_customer_id: str,
    stripe_product_price_plan_id: str,
    trial_period_in_days: int,
    metadata=None,
):
    # Create the subscription as per the product price plan.
    try:
        stripe_customer = stripe.Customer.retrieve(
            stripe_customer_id,
            expand=["subscriptions"],
        )
        found_subscription = False
        if stripe_customer and stripe_customer.get("subscriptions", {}).get("data", []):
            for subs in stripe_customer.get("subscriptions", {}).get("data", []):
                for item in subs.get("items", {}).get("data", []):
                    if (
                        item.get("plan", {}).get("id", "")
                        == stripe_product_price_plan_id
                    ):
                        found_subscription = True
                        break
        if found_subscription:
            logger.info(
                f"Already found a subscription for {stripe_customer_id}",
            )
            subscription_id = None
        else:
            subscription = stripe.Subscription.create(
                customer=stripe_customer_id,
                items=[
                    {"price": stripe_product_price_plan_id},
                ],
                trial_period_days=trial_period_in_days,
                trial_settings={
                    "end_behavior": {
                        "missing_payment_method": "cancel",
                    },
                },
                metadata=metadata,
            )
            subscription_id = subscription["id"]
    except Exception as e:
        logger.info(
            f"Error creating stripe subscription for {stripe_customer_id}"
            f" err: {traceback.format_exc()}, err_str: {str(e)}",
        )
        subscription_id = None

    return subscription_id


def handle_subscription_trial_end_event(event_object: dict):
    logger.info(f"Subscription Trial will end event for : {event_object['customer']}")
    # Update the end date in subscription
    nlm_user_id = event_object["metadata"].get("nlm_user_id", "")
    stripe_subscription_id = event_object["id"]

    if nlm_user_id and stripe_subscription_id:
        user = nosql_db.get_user(nlm_user_id, include_stripe_conf=True)
    else:
        logger.warning(
            f"Cannot find user information in the metadata. "
            f"Trying the customer ID route for {event_object['customer']}",
        )
        user = nosql_db.get_user_by_stripe_customer_id(
            event_object["customer"],
            include_stripe_conf=True,
        )
    if user:
        stripe_customer = stripe.Customer.retrieve(event_object["customer"])
        subscriptions = user.stripe_conf.get("subscriptions", [])
        status_changed = False
        for subs in subscriptions:
            if subs["stripe_subscription_id"] == stripe_subscription_id:
                if (
                    not event_object["default_payment_method"]
                    and not event_object["default_source"]
                    and not stripe_customer.get("invoice_settings", {}).get(
                        "default_payment_method",
                        "",
                    )
                    and not stripe_customer.get("default_source", "")
                ):
                    subs["status"] = "trialing_pending_payment_information"
                    status_changed = True
                break
        if status_changed:
            set_data = {
                "stripe_conf.subscriptions": subscriptions,
            }
            nosql_db.update_user(set_data, user_id=nlm_user_id)


def handle_subscription_updated_event(event_object: dict):
    logger.info(f"Subscription updated event for : {event_object['customer']}")
    # Update the end date in subscription
    if event_object["status"] in ["active", "past_due"]:
        nlm_user_id = event_object["metadata"].get("nlm_user_id", "")
        stripe_product_plan_id = event_object["metadata"].get(
            "stripe_product_plan_id",
            "",
        )
        resource_id = event_object["metadata"].get("resource_id", "")
        if nlm_user_id and stripe_product_plan_id:
            user = nosql_db.get_user(nlm_user_id, include_stripe_conf=True)
            if not user:
                logger.info(f"Not found the user with id {nlm_user_id} in this setup")
                return

            subscriptions = user.stripe_conf.get("subscriptions", [])
            for subs in subscriptions:
                if (
                    subs["stripe_product_plan_id"] == stripe_product_plan_id
                    and resource_id == subs["stripe_resource_id"]
                ):
                    if event_object["status"] == "active":
                        subs["end_date"] = date.fromtimestamp(
                            event_object["current_period_end"],
                        ).strftime("%Y-%m-%d")
                        subs["status"] = "active"
                    else:
                        subs["status"] = "past_due"
                    break

            set_data = {
                "stripe_conf.subscriptions": subscriptions,
            }

            active_subscriptions = 0
            for subs in subscriptions:
                if subs["status"] in ["active", "trialing_active"]:
                    active_subscriptions += 1
            if active_subscriptions >= 1:
                # Upgrade the metered subscription.
                app_name = event_object["metadata"].get("app_name", "")
                if app_name:
                    app_settings = nosql_db.get_application_settings(app_name)
                    set_data["subscription_plan"] = app_settings.get("users", {}).get(
                        "default_paid_metered_plan",
                        DEFAULT_SUBSCRIPTION_PLAN,
                    )
                    if PAYMENT_CONTROLLED_RENEWABLE_RESOURCES:
                        nosql_db.reset_renewable_resources(
                            user.id,
                            set_data["subscription_plan"],
                        )

            nosql_db.update_user(set_data, user_id=nlm_user_id)


def handle_subscription_deleted_event(event_object: dict):
    logger.info(f"Subscription deleted event for : {event_object['customer']}")
    if event_object["status"] == "canceled":
        # Remove the subscription for the workspace.
        nlm_user_id = event_object["metadata"].get("nlm_user_id", "")
        stripe_subscription_id = event_object["id"]
        resource_id = event_object["metadata"].get("resource_id", "")
        if nlm_user_id and stripe_subscription_id:
            user = nosql_db.get_user(nlm_user_id, include_stripe_conf=True)
        else:
            logger.warning(
                f"Cannot find user information in the metadata. "
                f"Trying the customer ID route for {event_object['customer']}",
            )
            user = nosql_db.get_user_by_stripe_customer_id(
                event_object["customer"],
                include_stripe_conf=True,
            )
        if user:
            subscriptions = user.stripe_conf.get("subscriptions", [])
            subscribed_workspaces = user.subscribed_workspaces or []
            restricted_workspaces = user.restricted_workspaces or []
            subscriptions = [
                subs
                for subs in subscriptions
                if subs["stripe_subscription_id"] != stripe_subscription_id
            ]
            if event_object["metadata"].get("resource" "") == "workspace":
                subscribed_workspaces = [
                    ws for ws in subscribed_workspaces if ws != resource_id
                ]
                if resource_id and resource_id not in restricted_workspaces:
                    restricted_workspaces.append(resource_id)
            # Update the user info object.
            set_data = {
                "stripe_conf.subscriptions": subscriptions,
                "subscribed_workspaces": subscribed_workspaces,
                "restricted_workspaces": restricted_workspaces,
            }
            if not subscriptions:
                # There are no more subscriptions. Downgrade the user to default subscription.
                app_name = event_object["metadata"].get("app_name", "")
                if app_name:
                    app_settings = nosql_db.get_application_settings(app_name)
                    set_data["subscription_plan"] = app_settings.get("users", {}).get(
                        "default_subscription_plan",
                        DEFAULT_SUBSCRIPTION_PLAN,
                    )
            logger.info(f"Updating User information... {set_data} ... {nlm_user_id}")
            nosql_db.update_user(set_data, user_id=nlm_user_id)


def handle_checkout_session_completed(event_object: dict):
    logger.info(f"Checkout session completed event for : {event_object['customer']}")
    if event_object["status"] == "complete":
        handle_checkout_success(event_object["id"])


def webhook_received():
    event = None
    payload = request.data

    try:
        sig_header = request.headers.get("stripe-signature")
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=STRIPE_WEBHOOK_ENDPOINT_SECRET,
        )
    except stripe.error.SignatureVerificationError as e:
        logger.warning(
            f"Webhook signature verification failed. {str(e)}"
            f" err: {traceback.format_exc()}",
        )
        logger.info(f"Webhook signature verification failed: {payload}")
        return jsonify(success=False)
    except Exception as e:
        logger.warning(
            f"Generic webhook failure. {str(e)}" f" err: {traceback.format_exc()}",
        )
        logger.info(f"Generic webhook failure: {payload}")
        return jsonify(success=False)

    if not event:
        logger.info(f"Cannot generate event from payload: {payload}")
        return jsonify(success=False)

    data = event["data"]
    event_type = event["type"]
    data_object = data["object"]

    # print("event " + event_type, data_object)
    # Checkout Session completed will be handled by the callback.
    # if event_type == "checkout.session.completed":
    #     handle_checkout_session_completed(data_object)
    if event_type == "customer.subscription.trial_will_end":
        handle_subscription_trial_end_event(data_object)
    elif event_type == "customer.subscription.updated":
        handle_subscription_updated_event(data_object)
    elif event_type == "customer.subscription.deleted":
        handle_subscription_deleted_event(data_object)
    else:
        logger.info(f"Unhandled event received {event_type}")

    return jsonify(success=True)
