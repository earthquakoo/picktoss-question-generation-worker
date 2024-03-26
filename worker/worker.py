import os
import json
import logging
from datetime import datetime

from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.llm.exception import InvalidLLMJsonResponseError
from core.enums.enum import LLMErrorType, SubscriptionPlanType, QuizQuestionNum, DocumentStatus
from core.database.database_manager import DatabaseManager
from core.llm.utils import fill_message_placeholders, load_prompt_messages

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    print(f"event: {event}")
    print(f"context: {context}")
    event_info: str = event["Records"][0]["body"]
    body: dict = json.loads(event_info)
    if "s3_key" not in body or "db_pk" not in body or "subscription_plan" not in body:
        raise ValueError(f"s3_key and db_pk and subscription_plan must be provided. event: {event}, context: {context}")
    
    s3_key = body["s3_key"]
    db_pk = int(body["db_pk"])
    subscription_plan = body["subscription_plan"]
    # core client settings
    s3_client = S3Client(access_key=os.environ["PICKTOSS_AWS_ACCESS_KEY"], secret_key=os.environ["PICKTOSS_AWS_SECRET_KEY"], region_name="us-east-1", bucket_name=os.environ["PICKTOSS_S3_BUCKET_NAME"])
    discord_client = DiscordClient(bot_token=os.environ["PICKTOSS_DISCORD_BOT_TOKEN"], channel_id=os.environ["PICKTOSS_DISCORD_CHANNEL_ID"])
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])
    chat_llm = OpenAIChatLLM(api_key=os.environ["PICKTOSS_OPENAI_API_KEY"], model="gpt-3.5-turbo-0125")
    # Retrieve document from S3
    bucket_obj = s3_client.get_object(key=s3_key)
    content = bucket_obj.decode_content_str()

    # Generate Questions

    CHUNK_SIZE = 1100
    chunks: list[str] = []
    for i in range(0, len(content), CHUNK_SIZE):
        chunks.append(content[i : i + CHUNK_SIZE])

    without_placeholder_messages = load_prompt_messages("/var/task/core/llm/prompts/generate_questions.txt") # dev & prod
    # without_placeholder_messages = load_prompt_messages("core/llm/prompts/generate_questions.txt") # local
    free_plan_question_expose_count = 0
    total_generated_question_count = 0

    success_at_least_once = False
    failed_at_least_once = False

    prev_questions: list[str] = []
    for chunk in chunks:
        prev_question_str = '\n'.join([q for q in prev_questions])
        messages = fill_message_placeholders(messages=without_placeholder_messages, placeholders={"note": chunk, "prev_questions": prev_question_str})
        try:
            resp_dict = chat_llm.predict_json(messages)
        except InvalidLLMJsonResponseError as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.INVALID_JSON_FORMAT,
                document_content=chunk,
                llm_response=e.llm_response,
                error_message="LLM Response is not JSON-decodable",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue
        except Exception as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.GENERAL,
                document_content=chunk,
                error_message="Failed to generate questions",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue

        try:
            for q_set in resp_dict:
                question, answer = q_set["question"], q_set["answer"]

                # To avoid duplication
                prev_questions.append(question)
                if len(prev_questions) == 6:
                    prev_questions.pop(0)

                total_generated_question_count += 1

                if subscription_plan == SubscriptionPlanType.FREE.value:
                    if free_plan_question_expose_count >= QuizQuestionNum.FREE_PLAN_QUIZ_QUESTION_NUM.value:
                        delivered_count = 0
                    else:
                        delivered_count = 1
                        free_plan_question_expose_count += 1
                elif subscription_plan == SubscriptionPlanType.PRO.value:
                    delivered_count = 1
                else:
                    raise ValueError("Wrong subscription plan type")
                print("3")
                question_insert_query = "INSERT INTO question (question, answer, document_id, delivered_count, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
                timestamp = datetime.now()
                db_manager.execute_query(question_insert_query, (question, answer, db_pk, delivered_count, timestamp, timestamp))
                db_manager.commit()

        except Exception as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.GENERAL,
                document_content=chunk,
                error_message=f"LLM Response is JSON decodable but does not have 'question' and 'answer' keys.\nresp_dict: {resp_dict}",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue

        success_at_least_once = True

        # Save generated question sets to database
        db_manager.commit()
    # Failed at every single generation
    if not success_at_least_once:
        document_update_query = "UPDATE document SET status = %s WHERE id = %s"
        db_manager.execute_query(document_update_query, (DocumentStatus.COMPLETELY_FAILED.value, db_pk))
        db_manager.commit()
        return

    # Failed at least one chunk question generation
    if failed_at_least_once:
        document_update_query = "UPDATE document SET status = %s WHERE id = %s"
        db_manager.execute_query(document_update_query, (DocumentStatus.PARTIAL_SUCCESS.value, db_pk))

    else:  # ALL successful
        document_update_query = "UPDATE document SET status = %s WHERE id = %s"
        db_manager.execute_query(document_update_query, (DocumentStatus.PROCESSED.value, db_pk))
        
    db_manager.commit()

    # Generate Summary
    summary_input = ""
    for chunk in chunks:
        summary_input += chunk[:600]
        if len(summary_input) > 2000:
            break

    without_placeholder_summary_messages = load_prompt_messages(
        "/var/task/core/llm/prompts/generate_summary.txt"
    ) # dev & prod
    # without_placeholder_summary_messages = load_prompt_messages(
    #     "core/llm/prompts/generate_summary.txt"
    # ) # local
    messages = fill_message_placeholders(
        messages=without_placeholder_summary_messages, placeholders={"note": summary_input}
    )
    try:
        resp_dict = chat_llm.predict_json(messages)
        summary = resp_dict["summary"]
    except InvalidLLMJsonResponseError as e:
        discord_client.report_llm_error(
            task="Summary Generation",
            error_type=LLMErrorType.INVALID_JSON_FORMAT,
            document_content=summary_input,
            llm_response=e.llm_response,
            error_message="LLM Response is not JSON-decodable",
            info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
        )
        return
    except Exception as e:
        discord_client.report_llm_error(
            task="Summary Generation",
            error_type=LLMErrorType.GENERAL,
            document_content=summary_input,
            error_message="Failed to generate questions",
            info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
        )
        return
    
    document_update_query = "UPDATE document SET summary = %s WHERE id = %s"
    db_manager.execute_query(document_update_query, (summary, db_pk))
    db_manager.commit()
    db_manager.close()

    return {"statusCode": 200, "message": "hi"}