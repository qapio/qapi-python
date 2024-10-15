from reactivex import operators, interval, compose
import time

interval(0.1).pipe(operators.take(1), compose(operators.map(lambda x: interval(0.5)), operators.switch_latest())).subscribe(lambda t: print(t, flush=True))


run = True

try:
    while run:
        time.sleep(1)  # Keep the program running and alive
except KeyboardInterrupt:
    print("Program terminated.")
    run = False