class BaseRow(object):
  def serialize(self):
    raise NotImplementedError("Row model needs to define a serialize function")
