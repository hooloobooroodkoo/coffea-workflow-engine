from coffea.processor import ProcessorABC

class MyProcessor(ProcessorABC):
    def process(self, events):
        return {"nevents": len(events)}

    def postprocess(self, accumulator):
        return accumulator