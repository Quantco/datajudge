def check_module_installed(module_name: str) -> bool:
    import importlib

    try:
        importlib.import_module(module_name)
        return True
    except ModuleNotFoundError:
        return False
