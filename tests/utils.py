from dagster import ExecuteInProcessResult


def get_saved_path(result: ExecuteInProcessResult, asset_name: str) -> str:
    print("hello")
    path = (
        list(filter(lambda evt: evt.is_handled_output, result.events_for_node(asset_name)))[0]
        .event_specific_data.metadata["path"]  # type: ignore
        .value
    )  # type: ignore[index,union-attr]
    assert isinstance(path, str)
    return path
