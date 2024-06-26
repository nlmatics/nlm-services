from server import util
from server.models.answer_content import AnswerContent  # noqa: F401,E501
from server.models.base_model_ import Model
from server.models.search_criteria import SearchCriteria  # noqa: F401,E501


class TrainSample(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(
        self,
        doc_id: str = None,
        workspace_id: str = None,
        id: str = None,
        created_on: str = None,
        train_state: str = None,
        model_to_train: str = None,
        criteria: SearchCriteria = None,
        top_answer: AnswerContent = None,
        selected_answer: AnswerContent = None,
    ):  # noqa: E501
        """TrainSample - a model defined in Swagger

        :param doc_id: The doc_id of this TrainSample.  # noqa: E501
        :type doc_id: str
        :param workspace_id: The workspace_id of this TrainSample.  # noqa: E501
        :type workspace_id: str
        :param id: The id of this TrainSample.  # noqa: E501
        :type id: str
        :param created_on: The created_on of this TrainSample.  # noqa: E501
        :type created_on: str
        :param train_state: The train_state of this TrainSample.  # noqa: E501
        :type train_state: str
        :param model_to_train: The model_to_train of this TrainSample.  # noqa: E501
        :type model_to_train: str
        :param criteria: The criteria of this TrainSample.  # noqa: E501
        :type criteria: SearchCriteria
        :param top_answer: The top_answer of this TrainSample.  # noqa: E501
        :type top_answer: AnswerContent
        :param selected_answer: The selected_answer of this TrainSample.  # noqa: E501
        :type selected_answer: AnswerContent
        """
        self.swagger_types = {
            "doc_id": str,
            "workspace_id": str,
            "id": str,
            "created_on": str,
            "train_state": str,
            "model_to_train": str,
            "criteria": SearchCriteria,
            "top_answer": AnswerContent,
            "selected_answer": AnswerContent,
        }

        self.attribute_map = {
            "doc_id": "docId",
            "workspace_id": "workspaceId",
            "id": "id",
            "created_on": "created_on",
            "train_state": "train_state",
            "model_to_train": "model_to_train",
            "criteria": "criteria",
            "top_answer": "topAnswer",
            "selected_answer": "selectedAnswer",
        }
        self._doc_id = doc_id
        self._workspace_id = workspace_id
        self._id = id
        self._created_on = created_on
        self._train_state = train_state
        self._model_to_train = model_to_train
        self._criteria = criteria
        self._top_answer = top_answer
        self._selected_answer = selected_answer

    @classmethod
    def from_dict(cls, dikt) -> "TrainSample":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The TrainSample of this TrainSample.  # noqa: E501
        :rtype: TrainSample
        """
        return util.deserialize_model(dikt, cls)

    @property
    def doc_id(self) -> str:
        """Gets the doc_id of this TrainSample.


        :return: The doc_id of this TrainSample.
        :rtype: str
        """
        return self._doc_id

    @doc_id.setter
    def doc_id(self, doc_id: str):
        """Sets the doc_id of this TrainSample.


        :param doc_id: The doc_id of this TrainSample.
        :type doc_id: str
        """

        self._doc_id = doc_id

    @property
    def workspace_id(self) -> str:
        """Gets the workspace_id of this TrainSample.


        :return: The workspace_id of this TrainSample.
        :rtype: str
        """
        return self._workspace_id

    @workspace_id.setter
    def workspace_id(self, workspace_id: str):
        """Sets the workspace_id of this TrainSample.


        :param workspace_id: The workspace_id of this TrainSample.
        :type workspace_id: str
        """

        self._workspace_id = workspace_id

    @property
    def id(self) -> str:
        """Gets the id of this TrainSample.


        :return: The id of this TrainSample.
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id: str):
        """Sets the id of this TrainSample.


        :param id: The id of this TrainSample.
        :type id: str
        """

        self._id = id

    @property
    def created_on(self) -> str:
        """Gets the created_on of this TrainSample.


        :return: The created_on of this TrainSample.
        :rtype: str
        """
        return self._created_on

    @created_on.setter
    def created_on(self, created_on: str):
        """Sets the created_on of this TrainSample.


        :param created_on: The created_on of this TrainSample.
        :type created_on: str
        """

        self._created_on = created_on

    @property
    def train_state(self) -> str:
        """Gets the train_state of this TrainSample.


        :return: The train_state of this TrainSample.
        :rtype: str
        """
        return self._train_state

    @train_state.setter
    def train_state(self, train_state: str):
        """Sets the train_state of this TrainSample.


        :param train_state: The train_state of this TrainSample.
        :type train_state: str
        """

        self._train_state = train_state

    @property
    def model_to_train(self) -> str:
        """Gets the model_to_train of this TrainSample.


        :return: The model_to_train of this TrainSample.
        :rtype: str
        """
        return self._model_to_train

    @model_to_train.setter
    def model_to_train(self, model_to_train: str):
        """Sets the model_to_train of this TrainSample.


        :param model_to_train: The model_to_train of this TrainSample.
        :type model_to_train: str
        """

        self._model_to_train = model_to_train

    @property
    def criteria(self) -> SearchCriteria:
        """Gets the criteria of this TrainSample.


        :return: The criteria of this TrainSample.
        :rtype: SearchCriteria
        """
        return self._criteria

    @criteria.setter
    def criteria(self, criteria: SearchCriteria):
        """Sets the criteria of this TrainSample.


        :param criteria: The criteria of this TrainSample.
        :type criteria: SearchCriteria
        """

        self._criteria = criteria

    @property
    def top_answer(self) -> AnswerContent:
        """Gets the top_answer of this TrainSample.


        :return: The top_answer of this TrainSample.
        :rtype: AnswerContent
        """
        return self._top_answer

    @top_answer.setter
    def top_answer(self, top_answer: AnswerContent):
        """Sets the top_answer of this TrainSample.


        :param top_answer: The top_answer of this TrainSample.
        :type top_answer: AnswerContent
        """

        self._top_answer = top_answer

    @property
    def selected_answer(self) -> AnswerContent:
        """Gets the selected_answer of this TrainSample.


        :return: The selected_answer of this TrainSample.
        :rtype: AnswerContent
        """
        return self._selected_answer

    @selected_answer.setter
    def selected_answer(self, selected_answer: AnswerContent):
        """Sets the selected_answer of this TrainSample.


        :param selected_answer: The selected_answer of this TrainSample.
        :type selected_answer: AnswerContent
        """

        self._selected_answer = selected_answer
