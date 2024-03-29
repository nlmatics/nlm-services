import logging
import traceback

import connexion
import jose
from flask import jsonify
from flask import make_response
from flask import redirect
from flask import request

from server.auth import auth_provider
from server.storage import nosql_db
from server.utils import auth_utils

logger = logging.getLogger()


def auth_callback_post():
    return auth_callback()


def auth_callback():
    try:
        tokens = auth_provider.callback(request)
        access_token, refresh_tok = tokens["access_token"], tokens["refresh_token"]

        response = make_response(redirect(auth_provider.FRONTEND_URL))
        add_cookie_same_site = False
        # Add SameSite Cookie setting for loading the plugin in iframe.
        payload = auth_provider.verify_token(access_token)
        app_name = payload.get("https://nlmatics.com/application_name", None)
        if app_name:
            app_settings = nosql_db.get_application_settings(app_name)
            if app_settings and app_settings.get("add_cookie_same_site", False):
                add_cookie_same_site = True

        # set user profiles to cookies
        user = auth_provider.auth_user(
            access_token,
            return_user=True,
            check_last_login_time=False,
        )

        response.set_cookie(
            "user_id",
            user.id,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )
        response.set_cookie(
            "email",
            user.email_id,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )
        response.set_cookie(
            "first_name",
            user.first_name,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )
        response.set_cookie(
            "last_name",
            user.last_name,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )

        # set tokens to cookies
        response.set_cookie(
            "access_token",
            access_token,
            max_age=auth_provider.ACCESS_TOKEN_EXPIRE_SECONDS,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )
        response.set_cookie(
            "refresh_token",
            refresh_tok,
            max_age=auth_provider.REFRESH_TOKEN_EXPIRE_SECONDS,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )
        logger.info(f"Login: email: {user.email_id}, user_id: {user.id}")
        # Update the last login
        auth_utils.update_last_login(user_id=user.id)
        return response

    except jose.JOSEError as e:  # the catch-all of Jose
        logger.error(f"Error during verify token: {e}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 403)
    except Exception as e:
        logger.error(f"Callback: Error during verify token: {e}")
        if isinstance(e.args, tuple):
            error, error_code = e.args
            response = make_response(
                redirect(
                    auth_provider.logout_url(error, error_code),
                ),
            )
            return response
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 403)


def refresh_token():
    try:
        refresh_tok = connexion.request.get_json()["refresh_token"]
        tokens = auth_provider.generate_token(token=refresh_tok)
        email = tokens.get("email", "")
        del tokens["email"]
        response = make_response(
            jsonify(tokens),
            200,
        )
        access_token, refresh_tok = tokens["access_token"], tokens["refresh_token"]
        add_cookie_same_site = False
        # Add SameSite Cookie setting for loading the plugin in iframe.
        payload = auth_provider.verify_token(access_token)
        app_name = payload.get("https://nlmatics.com/application_name", None)
        if app_name:
            app_settings = nosql_db.get_application_settings(app_name)
            if app_settings and app_settings.get("add_cookie_same_site", False):
                add_cookie_same_site = True

        # set tokens to cookies
        response.set_cookie(
            "access_token",
            access_token,
            max_age=auth_provider.ACCESS_TOKEN_EXPIRE_SECONDS,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )
        response.set_cookie(
            "refresh_token",
            refresh_tok,
            max_age=auth_provider.REFRESH_TOKEN_EXPIRE_SECONDS,
            domain=auth_provider.domain,
            httponly=False,
            secure=True,
            samesite="None" if add_cookie_same_site else None,
        )
        if email:
            auth_utils.update_last_login(email=email)
        return response
    except jose.JOSEError as e:  # the catch-all of Jose
        logger.error(f"Error during verify token: {e}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 403)
    except Exception as e:
        logger.error(
            f"Error in refresh_token, err: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def login_url():
    url = auth_provider.login_url()

    for key in ["state", "nonce"]:
        if key in request.args:
            url += f"&{key}={request.args[key]}"

    return url


def logout_url():
    return auth_provider.logout_url()


def login():
    response = make_response(redirect(auth_provider.login_url()))
    return response


def logout(error_code: str = None):
    cookies = connexion.request.cookies

    user_id = cookies.get("user_id", None)
    ref_token = cookies.get("refresh_token", None)
    if user_id and ref_token:
        auth_utils.update_logged_in(user_id=user_id, is_logged_in=False)
        auth_provider.revoke_refresh_token(ref_token)

    redirect_url = auth_provider.FRONTEND_URL
    if error_code:
        redirect_url = f"{auth_provider.FRONTEND_URL}?errorCode={error_code}"
    response = make_response(redirect(redirect_url))
    # remove user info from cookies
    response.delete_cookie("user_id", domain=auth_provider.domain)
    response.delete_cookie("email", domain=auth_provider.domain)
    response.delete_cookie("first_name", domain=auth_provider.domain)
    response.delete_cookie("last_name", domain=auth_provider.domain)

    # remove tokens from cookies
    response.delete_cookie("access_token", domain=auth_provider.domain)
    response.delete_cookie("refresh_token", domain=auth_provider.domain)
    return response
