"""
Calcul factorial paralel folosind multiprocessing.

Tema 1: calculează n! simultan pentru mai multe valori,
folosind atât multiprocessing.Queue + Process cât și ProcessPoolExecutor.
"""

import multiprocessing
from concurrent.futures import ProcessPoolExecutor


def factorial(n: int) -> int:
    """Calculează n! (factorial).

    Args:
        n: Numărul pentru care se calculează factorialul. Trebuie să fie >= 0.

    Returns:
        n! ca întreg.

    Raises:
        ValueError: Dacă n este negativ.

    Exemple:
        factorial(0) == 1
        factorial(1) == 1
        factorial(5) == 120
    """
    if n < 0:
        raise ValueError("n trebuie să fie >= 0")
    if n == 0:
        return 1
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


def _worker_factorial(input_queue: multiprocessing.Queue, output_queue: multiprocessing.Queue) -> None:
    """Funcție worker pentru procesul multiprocessing.

    Citește valori din input_queue, calculează factorialul și trimite
    rezultatele în output_queue sub forma (n, factorial(n)).

    Se oprește când primește None din input_queue.

    Args:
        input_queue: Coada de unde se citesc valorile n.
        output_queue: Coada unde se trimit perechile (n, rezultat).
    """
    while True:
        n = input_queue.get()
        if n is None:
            break
        output_queue.put((n, factorial(n)))


def parallel_factorial_multiprocessing(values: list[int]) -> dict[int, int]:
    """Calculează factorialul pentru mai multe valori în paralel.

    Folosește 4 procese worker cu multiprocessing.Queue și multiprocessing.Process.

    Args:
        values: Lista valorilor pentru care se calculează factorialul.

    Returns:
        Dict {n: factorial(n)} pentru toate valorile din lista.

    Exemplu:
        result = parallel_factorial_multiprocessing([5, 6, 7, 8])
        # {5: 120, 6: 720, 7: 5040, 8: 40320}
    """
    if not values:
        return {}

    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()

    num_workers = 4
    processes = [
        multiprocessing.Process(target=_worker_factorial, args=(input_queue, output_queue))
        for _ in range(num_workers)
    ]

    for p in processes:
        p.start()

    for v in values:
        input_queue.put(v)

    for _ in range(num_workers):
        input_queue.put(None)

    rezultat = {}
    for _ in range(len(values)):
        n, fact = output_queue.get()
        rezultat[n] = fact

    for p in processes:
        p.join()

    return rezultat


def parallel_factorial_futures(values: list[int]) -> dict[int, int]:
    """Calculează factorialul pentru mai multe valori în paralel.

    Folosește concurrent.futures.ProcessPoolExecutor cu max_workers=4.

    Args:
        values: Lista valorilor pentru care se calculează factorialul.

    Returns:
        Dict {n: factorial(n)} pentru toate valorile din lista.

    Exemplu:
        result = parallel_factorial_futures([5, 6, 7, 8])
        # {5: 120, 6: 720, 7: 5040, 8: 40320}
    """
    if not values:
        return {}

    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(factorial, n): n for n in values}
        return {n: future.result() for future, n in futures.items()}