import openai
from backend.config import settings
from backend.prompt import build_sql_prompt


client = openai.OpenAI(api_key=settings.openai_api_key)


class LLMError(Exception):
    pass


def generate_sql(question: str) -> str:
    """
    Send the question to GPT and return the generated SQL string.
    Raises LLMError if the call fails or the model signals it cannot generate.
    """
    prompt = build_sql_prompt(question)

    try:
        response = client.chat.completions.create(
            model=settings.openai_model,
            max_tokens=settings.openai_max_tokens,
            temperature=settings.openai_temperature,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )
    except openai.OpenAIError as e:
        raise LLMError(f"OpenAI API error: {e}") from e

    raw = response.choices[0].message.content.strip()

    if raw.startswith("CANNOT_GENERATE:"):
        reason = raw.removeprefix("CANNOT_GENERATE:").strip()
        raise LLMError(f"Model could not generate SQL: {reason}")

    return raw
