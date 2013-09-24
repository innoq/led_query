require "erubis"
require "led_query"
require "led_query/sparql"

class LEDQuery::Database

  class Link # XXX: does not belong here
    attr_reader :uri

    def initialize(uri, label)
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

  end

  # `triplestore` is the URL of the Sesame repository (typically
  # .../openrdf-sesame/repositories/my-repo`)
  # `logger` is an optional Logger (or compatbile) instance
  def initialize(triplestore, logger=nil)
    @triplestore = triplestore
    @logger = logger
  end

  # determine observations for the given concepts and from the given sources
  # (all URIs)
  # returns a list of hashes representing individual observations
  def determine_observations(concepts_by_dimension, include_descendants=false)
    query = make_query("determine_observations", {
      :include_descendants => include_descendants,
      :concepts_by_dimension => concepts_by_dimension.
          inject({}) do |memo, (dim, concepts)|
        memo[dim] = resource_list(concepts)
        memo
      end
    })
    log :info, "querying observations"
    res = sparql(query, include_descendants)
    return res["results"]["bindings"].map do |result| # TODO: error handling
      analyte_label = result["albl"]["value"] rescue nil
      location_label = result["llbl"]["value"] rescue nil
      source_label = result["dlbl"]["value"] rescue nil
      {
        "obs" => result["obs"]["value"],
        # XXX: hard-coding data types for now
        "mean" => (Float(result["mean"]["value"]) rescue nil),
        "uom" => (result["uom"]["value"] rescue nil),
        "title" => (result["title"]["value"] rescue nil),
        "desc" => (result["desc"]["value"] rescue nil),
        "analyte" => Link.new(result["analyte"]["value"], analyte_label),
        "location" => Link.new(result["location"]["value"], location_label),
        "source" => Link.new(result["dataset"]["value"], source_label),
        "time" => ["startTime", "endTime"].map do |key|
          Float(result[key]["value"]).to_i
        end
      }
    end
  end

  # determine concepts from the given dimension (all URIs), optionally limited
  # to those co-occurring with the given set of concepts from other dimensions
  # returns a hash of concepts by type - concepts are URI/labels pairs, with
  # labels indexed by language
  def determine_concepts(dimension, concepts_by_dimension={},
      include_observations_count=false, include_hierarchy=false,
      include_descendants=false) # TODO: refactor, improve API
    infer = include_hierarchy || include_descendants

    bindings = ["?type", "?concept", "?label"]
    if include_hierarchy
      bindings += ["?grancestor", "?ancestor", "?parent", "?ancLabel"]
    end

    query_params = {
      :dimension => "<#{dimension}>",
      :bindings => bindings,
      :include_hierarchy => include_hierarchy,
      :include_descendants => include_descendants,
      :pre_existing_conditions => concepts_by_dimension.
          inject({}) do |memo, (dim, concepts)|
        memo[dim] = resource_list(concepts)
        memo
      end
    }

    register_label = lambda do |hash, concept, label, always=false|
      hash[concept] ||= {} if label || always
      if label
        lang = label["xml:lang"]
        hash[concept][lang] = label["value"]
      end
    end

    query = make_query("determine_concepts", query_params)
    log :info, "querying concepts"
    res = sparql(query, infer)
    concepts_by_type = res["results"]["bindings"].inject({}) do |memo, result| # TODO: error handling
      type = result["type"]["value"]
      concept = result["concept"]["value"]
      memo[type] ||= {}
      register_label.call(memo[type], concept, result["label"], true)
      if include_hierarchy
        if ancestor = result["ancestor"]
          register_label.call(memo[type], ancestor["value"], result["ancLabel"])
        end
        memo["_hierarchy"] ||= []
        memo["_hierarchy"] << ["grancestor", "ancestor", "parent", "concept"].
            map { |key| result[key]["value"] rescue nil }
      end
      memo
    end
    hierarchy = concepts_by_type.delete("_hierarchy")

    if include_observations_count
      query_params["bindings"] = ["(COUNT(DISTINCT ?obs) AS ?obsCount)"]
      query = make_query("determine_concepts", query_params)
      res = sparql(query, infer)
      obs_count = Float(res["results"]["bindings"][0]["obsCount"]["value"]).to_i
      ret = [concepts_by_type, obs_count]
    else
      ret = [concepts_by_type]
    end
    ret << self.class.resolve_hierarchy(hierarchy) if include_hierarchy
    return ret.length == 1 ? ret[0] : ret
  end

  # returns a hash of URI/labels pairs, with labels indexed by language
  def determine_dimensions
    query = make_query("determine_dimensions")
    log :info, "querying dimensions"
    res = sparql(query)
    return res["results"]["bindings"].inject({}) do |memo, result| # TODO: error handling
      id = result["dim"]["value"]
      memo[id] ||= {}
      if label = result["label"]
        lang = label["xml:lang"]
        memo[id][lang] = label["value"]
      end
      memo
    end
  end

  def observations_count(concepts_by_dimension={}, include_descendants=false)
    query = make_query("determine_observations_count", { # XXX: largely duplicates `determine_observations`
      :include_descendants => include_descendants,
      :concepts_by_dimension => concepts_by_dimension.
          inject({}) do |memo, (dim, concepts)|
        memo[dim] = resource_list(concepts)
        memo
      end
    })
    log :info, "querying observations count"
    res = sparql(query, include_descendants)
    return Float(res["results"]["bindings"][0]["obsCount"]["value"]).to_i
  end

  # `var` is used as suffix to create pseudo-local variables
  def dimension_query(dim, var=nil, include_descendants=false) # TODO: rename
    if dim == "http://data.uba.de/led/temporal" # XXX: special-casing
      statements = <<-EOS.rstrip
    ?obs <#{dim}> ?time#{var} .
    ?time#{var} dct:start ?concept#{var} .
      EOS
    elsif include_descendants
      statements = <<-EOS.rstrip
    ?obs <#{dim}> ?subConcept#{var} .
    ?concept#{var} skos:narrower* ?subConcept#{var} .
      EOS
    else
      statements = <<-EOS.rstrip
    ?obs <#{dim}> ?concept#{var} .
      EOS
    end
    return "#{statements}\n    ?obs a qb:Observation ."
  end

  def resource_list(concepts)
    res = concepts.map do |concept|
      begin # number -- XXX: special-casing for years
        Float(concept).to_i
      rescue ArgumentError # URI
        "<#{concept}>"
      end
    end.join(", ")
    return res
  end

  def sparql(query, infer=false)
    return LEDQuery::SPARQL.query(@triplestore, query, infer, @logger)
  end

  def make_query(template, data=nil)
    templates_dir = File.expand_path(File.join("..", "templates"), __FILE__)
    render = lambda do |template, data| # required for partials -- XXX: hacky!
      path = File.join(templates_dir, "#{template}.sparql.erb")
      res = File.read(path)
      return Erubis::Eruby.new(res).result(data)
    end
    data ||= {}
    data["render"] = render
    return render.call(template, data)
  end

  def log(level, msg)
    @logger.send(level, msg) if @logger
  end

  # turns a list of grancestor/ancestor/parent/concept tuples into a nested hash
  # (grancestor is the respective ancestor's parent)
  def self.resolve_hierarchy(entries) # TODO: use TSort?
    hierarchy = { "_index" => {} }
    register = lambda do |id|
      return hierarchy["_index"][id] ||= {}
    end

    entries.each do |grancestor, ancestor, parent, concept|
      concept_node = register.call(concept)
      parent_node = register.call(parent)
      ancestor_node = register.call(ancestor)
      root = grancestor.nil? ? hierarchy : register.call(grancestor)

      parent_node[concept] ||= concept_node
      root[ancestor] ||= ancestor_node
    end

    hierarchy.delete("_index")
    return hierarchy
  end

end
