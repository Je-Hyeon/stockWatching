import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def summarize_report(report_text: str) -> str:
    try:
        resp = client.responses.create(
            model="gpt-4.1",
            input=[
                {"role": "developer",
                 "content": """당신은 한국 증권사 리포트를 요약하는 전문가입니다.
                    ### 작업 지침 ###
                    1. 아래 리포트(첫 페이지)를 읽고 핵심 내용을 **3~6문장**으로 요약하세요.
                    2. **수치(매출액·성장률 등)는 최소화**하고, 왜 그런 전망을 내놓았는지 **논리·근거** 위주로 작성하세요.
                    3. **목표주가(TP)와 투자의견**이 있으면 반드시 포함하세요.
                    4. 보고서 작성자의 면책 조항과 관련된 내용은 제외하세요.
                    5. GPT의 의견을 추가하지 말고, 철저히 글 내용 기반으로 답해주세요."""},
                {"role": "user", "content": report_text}
            ],
            temperature=0.1,
        )
        return resp.output_text.strip()
    except Exception as e:
        return f"요약 실패: {e}"