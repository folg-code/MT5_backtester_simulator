import importlib

def load_strategy(name: str, df):
    import importlib

    module_path = f"strategies.{name}"
    class_name = ''.join(part.capitalize() for part in name.split('_'))

    module = importlib.import_module(module_path)
    strategy_class = getattr(module, class_name)

    return strategy_class(df)