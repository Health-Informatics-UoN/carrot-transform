from ._version import __version__

params = {
    "version": __version__,
}


def require(con: bool, msg: str = ""):
    """this is a run-time check for various things that are needed, but, don't feel "worth" more elabourate checks"""

    # if the condition passes; return
    if con:
        return

    # prepare the message for output
    if msg == "":
        pass  # if there's no message; leave it as a short string
    else:
        # if there is a message;
        msg = "\n\t" + msg

    import inspect

    # Get the calling frame and its code context
    currentframe = inspect.currentframe()

    # most of the complexity here is because of mypy checks
    frame = currentframe.f_back if currentframe is not None else None
    frame_info = inspect.getframeinfo(frame) if frame is not None else None
    context = frame_info.code_context if frame_info is not None else None

    prefix: str
    if context and frame_info:
        call_line: str = context[0].strip()
        prefix = f"failed {frame_info.filename}:{frame_info.lineno}: {call_line}"
    elif frame_info is not None:
        prefix = f"failed {frame_info.filename}:{frame_info.lineno}"
    else:
        prefix = "failed requirement"

    raise AssertionError(prefix + msg)
