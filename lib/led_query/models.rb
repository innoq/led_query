require "led_query"

class LEDQuery::Observation
  attr_reader :uri, :extras
  attr_accessor :source, :medium, :analyte, :location, :time, :mean, :uom,
      :title, :desc

  def initialize(uri)
    @uri = uri
    @extras = {} # additional context (custom dimensions and attributes)

    # attributes default to sets
    slots = [:source, :medium, :analyte, :location, :time, :mean, :uom, :title,
        :desc] # XXX: duplicates `attr_accessor` above
    slots.each { |slot| instance_variable_set(:"@#{slot}", Set.new) }
  end

  def [](key)
    return self.send(key)
  end

  def []=(key, value)
    return self.send("#{key}=", value)
  end

  def is_metadata?
    return @mean.empty?
  end

end

class LEDQuery::Link
  attr_reader :uri, :label_lang

  def initialize(uri, label=nil, label_lang=nil)
    @uri = uri
    @label = label
    @lang = label_lang if @label
  end

  def label
    return @label || @uri
  end

  def eql?(other)
    self.to_s == other.to_s
  end

  def hash
    return [@uri, @label, @lang].hash
  end

  def to_s
    res = "<#{@uri}>"
    if @label
      lang = @lang ? "@#{@lang}" : nil
      res = [%("#{@label}"), lang, res].join("")
    end
    return res
  end

  def inspect
    return "#<#{self.class} #{self.to_s}.>"
  end

end
