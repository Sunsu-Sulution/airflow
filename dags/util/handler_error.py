import traceback
from util.lark_helper import send_message_to_lark

def handle_error(func):
    def runner(*args):
        try:
            func(*args)
            send_message_to_lark(f"Run task: `{func.__name__}` successfully")
        except Exception as e:
            send_message_to_lark(f"Error when running task: `{func.__name__}` with error\n\n```{traceback.format_exc()}```")
            raise e
    runner.__name__ = func.__name__
    return runner
