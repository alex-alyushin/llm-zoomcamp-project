import logging

def configure_logging(service_name: str):
    logging.basicConfig(
        level=logging.WARNING,
        format=f"%(asctime)s [%(levelname)s] {service_name:>10} | %(message)s"
    )
