from copy import deepcopy

from unittest.mock import patch
import pytest

from target_postgres import target_tools

from fixtures import CONFIG


def test_usage_stats():
    config = deepcopy(CONFIG)
    assert config['disable_collection']

    with patch.object(target_tools,
                      '_async_send_usage_stats') as mock:
        target_tools.stream_to_target([], None, config=config)

        assert mock.call_count == 0

        config['disable_collection'] = False

        target_tools.stream_to_target([], None, config=config)

        assert mock.call_count == 1
