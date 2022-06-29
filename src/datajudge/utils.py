def check_module_installed(module_name: str) -> bool:
    import importlib

    try:
        mod = importlib.import_module(module_name)
        return mod is not None
    except ModuleNotFoundError:
        return False
