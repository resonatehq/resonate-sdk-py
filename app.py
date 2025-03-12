
import threading


def main() -> None:
    l = threading.local()
    l.x = 1
    print(l.x)

if __name__ == "__main__":
    main()
