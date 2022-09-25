from .etl_interfaces import IETL, IExtracter, ILoader, ITransformer


class MoviesETL(IETL):
    def __init__(
        self,
        extracter: IExtracter,
        transformer: ITransformer,
        loader: ILoader,
    ):
        self.transformer = transformer
        self.extracter = extracter
        self.loader = loader

    def run(self):
        for extracted in self.extracter.extract():
            transformed = self.transformer.transform(extracted)
            self.loader.load(transformed)
            # etl saves states only after data extracted, transformed, loaded
            self.save_state()

    def save_state(self):
        self.extracter.save_state()
        self.transformer.save_state()
        self.loader.save_state()
