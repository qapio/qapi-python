import importlib.machinery
import inspect
import sys

module = importlib.machinery.SourceFileLoader("a", f"{sys.argv[1]}").load_module()

class_members = inspect.getmembers(module, inspect.isfunction)

order = ['before', 'factor', 'after']

def main(execution_context):
    data_context = {'members': ["A", "B", "C", "D"], 'dates': ["1", "2", "3"]}

    for stage in order:
        for member in class_members:

            meta = member[1].__annotations__
            role = meta.get("return")

            if role != stage:
                continue

            if  role == 'before':
                data_context[member[0]] = member[1](execution_context)

            if  role == 'factor':
                results = {}
                for d in execution_context['dates']:

                    results[d] = []

                    for m in execution_context['members']:
                        results[d].append(member[1](m, d, data_context))

                data_context[member[0]] = results

            if role == 'after':
                data_context[member[0]] = member[1](execution_context)


    return {}
    ## main( {'members': ["A", "B", "C", "D"], 'dates': ["1", "2", "3"]})