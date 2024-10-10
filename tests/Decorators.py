def before(fun):
    fun.stage_type = 'before'
    return fun

def after(fun):
    fun.stage_type = 'after'
    return fun

def factor(fun):
    fun.stage_type = 'factor'
    return fun