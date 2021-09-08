from config_parser import ConfigParser
from moc_jobs.tasks import MoCTasks


if __name__ == '__main__':

    config_parser = ConfigParser()
    moc_tasks = MoCTasks(
        config_parser.config,
        config_parser.config_network,
        config_parser.connection_network
    )
    moc_tasks.start_loop()
