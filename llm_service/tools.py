TOOLS = [
    {
        "type": "function",
        "name": "store_user_cv",
        "description": (
            "Persist the user's resume (CV) exactly as provided, without "
            "modification or summarization. Call this once, as soon as the "
            "current user message contains resume content, before doing "
            "anything else in your reply. Do not pass the resume text as an "
            "argument — the application already holds the raw file; this "
            "call is only a trigger to save it. Returns {\"status\": ...}; "
            "briefly tell the user whether the save succeeded or failed."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False
        }
    }
]
