require "erubis"
require "led_query"
require "led_query/models"
require "led_query/sparql"

class LEDQuery::Database

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
    log :info, "querying observations"
    res = sparql("determine_observations", {
      :include_descendants => include_descendants,
      :concepts_by_dimension => concepts_by_dimension.
          inject({}) do |memo, (dim, concepts)|
        memo[dim] = resource_list(concepts)
        memo
      end
    })

    observations = res["results"]["bindings"] rescue []
    return observations.inject({}) do |memo, result|
      uri = result["obs"]["value"]
      memo[uri] ||= LEDQuery::Observation.new(uri)
      obs = memo[uri]

      {
        "mean" => (Float(result["mean"]["value"]) rescue nil),
        "uom" => (result["uom"]["value"] rescue nil),
        "title" => (result["title"]["value"] rescue nil),
        "desc" => (result["desc"]["value"] rescue nil),
        "source" => make_link(result, "dataset", "dlbl"),
        "medium" => make_link(result, "medium", "mlbl"),
        "analyte" => make_link(result, "analyte", "albl"),
        "location" => make_link(result, "location", "llbl"),
        "time" => ["startTime", "endTime"].map do |key|
          Float(result[key]["value"]).to_i rescue nil
        end
      }.each do |attr, value|
        empty = value.is_a?(Array) ? value.compact.empty? : !value
        obs.send(attr) << value unless empty
      end

      memo
    end
  end

  # determine concepts from the given dimension (all URIs), optionally limited
  # to those co-occurring with the given set of concepts from other dimensions
  # returns a hash of concepts by type - concepts are URI/labels pairs, with
  # labels indexed by language
  # options (all false by default):
  # * `:include_observations_count`: adds observation count to return value
  # * `:include_hierarchy`: adds hierarchy to return value
  # * `:include_descendants`: consider concepts' descendants when determining
  #    co-occurrence (e.g. "animal" implicitly includes "cat" and "dog")
  # * `:infer`: force inferences
  # note that the return value changes depending on the selected options
  def determine_concepts(dimension, concepts_by_dimension={}, options={})
    include_observations_count = options[:include_observations_count] || false
    include_hierarchy = options[:include_hierarchy] || false
    include_descendants = options[:include_descendants] || false
    force_infer = options[:infer] || false

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

    log :info, "querying concepts"
    res = sparql("determine_concepts", query_params, force_infer)
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
      res = sparql("determine_concepts", query_params, force_infer)
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
    log :info, "querying dimensions"
    res = sparql("determine_dimensions")
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
    log :info, "querying observations count"
    res = sparql("determine_observations_count", { # XXX: largely duplicates `determine_observations`
      :include_descendants => include_descendants,
      :concepts_by_dimension => concepts_by_dimension.
          inject({}) do |memo, (dim, concepts)|
        memo[dim] = resource_list(concepts)
        memo
      end
    })

    return res["results"]["bindings"].inject({}) do |memo, result|
      dataset = result["dataset"]["value"] rescue nil
      memo[dataset] = {
        "count" => Float(result["obsCount"]["value"]).to_i,
        "label" => (result["label"]["value"] rescue nil)
      }
      memo
    end
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

  def make_link(result, uri_key, label_key)
    label = result[label_key]["value"] rescue nil
    return LEDQuery::Link.new(result[uri_key]["value"], label)
  end

  def sparql(query_template, query_params={}, force_infer=false)
    query, infer = LEDQuery::SPARQL.make_query(query_template, query_params)
    return _sparql(query, infer || force_infer)
  end

  def _sparql(query, infer)
    return LEDQuery::SPARQL.query(@triplestore, query, infer, @logger)
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
