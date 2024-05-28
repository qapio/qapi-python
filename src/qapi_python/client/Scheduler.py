from reactivex.scheduler import ThreadPoolScheduler, EventLoopScheduler
import multiprocessing

optimal_thread_count = multiprocessing.cpu_count()

scheduler = ThreadPoolScheduler(optimal_thread_count)
#scheduler = EventLoopScheduler()
