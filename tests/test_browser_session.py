from crawl_experiment.browser.seleniumbase_session import SeleniumBaseSession


class InvalidHandleProcess:
    _child_created = True

    def poll(self):
        error = OSError("invalid handle")
        error.winerror = 6
        raise error


def test_invalid_windows_process_handle_is_suppressed_after_shutdown():
    process = InvalidHandleProcess()

    SeleniumBaseSession._suppress_invalid_windows_handle(process)

    assert process._child_created is False
