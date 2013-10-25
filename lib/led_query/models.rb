require "led_query"

class LEDQuery::Observation
  attr_reader :uri
  attr_accessor :source, :medium, :analyte, :location, :time, :mean, :uom,
      :title, :desc

  alias_method :obs, :uri # XXX: required only for backwards compatibility

  def initialize(uri)
    @uri = uri
  end

  def [](key)
    return self.send(key)
  end

  def is_metadata?
    return !@mean
  end

end

class LEDQuery::Link
  attr_reader :uri

  def initialize(uri, label=nil)
    @uri = uri
    @label = label
  end

  def label
    return @label || @uri
  end

  def to_s
    res = "<#{@uri}>"
    return @label ? %("#{@label}"#{res}) : res
  end

  def inspect
    return "#<#{self.class} #{self.to_s}.>"
  end

end
