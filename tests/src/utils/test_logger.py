import logging

from src.utils.logger import get_logger


def test_returns_named_logger():
    logger = get_logger("tests.logger")

    assert isinstance(logger, logging.Logger)
    assert logger.name == "tests.logger"


def test_same_name_returns_same_logger():
    assert get_logger("tests.same") is get_logger("tests.same")


def test_messages_are_emitted(caplog):
    with caplog.at_level(logging.INFO):
        get_logger("tests.emit").info("Hello %s", "pipeline")

    assert ("tests.emit", logging.INFO, "Hello pipeline") in caplog.record_tuples
