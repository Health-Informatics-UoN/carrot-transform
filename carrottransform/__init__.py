from ._version import __version__

params = {
    "version": __version__,
}

def require(con: bool, msg: str = ""):
    if con:
        return

    if "" != msg:
        msg = "\n\t" + msg
    import inspect

    # Get the calling frame and its code context
    currentframe = inspect.currentframe()
    frame = currentframe.f_back if currentframe is not None else None
    frame_info = inspect.getframeinfo(frame) if frame is not None else None
    context = frame_info.code_context if frame_info is not None else None
    if context:
        call_line = context[0].strip()
        raise AssertionError(
            f"failed {frame_info.filename}:{frame_info.lineno}: {call_line}{msg}"
        )
    if frame_info is not None:
        raise AssertionError(f"failed {frame_info.filename}:{frame_info.lineno}{msg}")

    raise AssertionError(f"failed requirement{msg}")
