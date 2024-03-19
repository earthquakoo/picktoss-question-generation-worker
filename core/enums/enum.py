from enum import Enum

class LLMErrorType(Enum):
    INVALID_JSON_FORMAT = "INVALID_LLM_JSON_RESPONSE_FORMAT"
    GENERAL = "GENERAL"
    

class SubscriptionPlanType(Enum):
    FREE = "FREE"
    PRO = "PRO"


class QuizQuestionNum(Enum):
    FREE_PLAN_QUIZ_QUESTION_NUM = 5
    PRO_PLAN_QUIZ_QUESTION_NUM = 10


class DocumentStatus(Enum):
    UNPROCESSED = "UNPROCESSED"
    PROCESSED = "PROCESSED"
    COMPLETELY_FAILED = "COMPLETELY_FAILED"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"