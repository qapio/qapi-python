from reactivex import operators, interval, compose
import time

interval(0.1).pipe(operators.take(1), compose(operators.map(lambda x: interval(1).pipe(operators.with_latest_from(interval(0.01)))), operators.switch_latest())).subscribe(lambda t: print(t, flush=True))


a = {}

print(a.get("A"))