import logging

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def init_de_lite():
    from de_utils import load_settings
    from discovery_engine import DiscoveryEngine

    # from engine.services import ServiceFacade

    settings = load_settings()

    discovery_engine = DiscoveryEngine(settings, mode="pipeline")

    engines = [
        "TemplateEngine",
        "SearchEngine",
        "TableExtractionEngine",
        "RetrievalEngine",
        "AnsweringEngine",
        "BooleanEngine",
        "PostProcessorEngine",
        "AbstractiveProcessorEngine",
        "GroupingEngine",
        "PostRankingEngine",
        "AggregatePostProcessorEngine",
    ]

    for engine in engines:
        discovery_engine.add_engine(engine)

    discovery_engine.init_workers()
    return discovery_engine
