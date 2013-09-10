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
  def determine_observations(concepts_by_dimension)
    conditions = concepts_by_dimension.each_with_index. # XXX: largely duplicates `determine_concepts`
        map do |(dim, concepts), i|
      concepts = resource_list(concepts)
      [dimension_query(dim, i), "FILTER(?concept#{i} IN (#{concepts}))"].
          join("\n    ")
    end.join("\n")

    query = <<-EOS.strip
PREFIX dct:<http://purl.org/dc/terms/>
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX qb:<http://purl.org/linked-data/cube#>
PREFIX led:<http://data.uba.de/led/>

SELECT DISTINCT
    ?obs ?mean ?uom ?title ?desc ?analyte ?location ?startTime ?endTime ?dataset
    ?albl ?llbl ?dlbl
WHERE {
#{conditions}
    ?obs led:analyte ?analyte .
    ?obs led:location ?location .
    ?obs led:source ?dataset .
    ?obs a qb:Observation .
    OPTIONAL { ?obs led:mean ?mean } .
    OPTIONAL { ?obs led:uom ?uom } .
    OPTIONAL { ?obs dct:title ?title } .
    OPTIONAL { ?obs dct:description ?desc } .
    ?obs led:temporal ?time .
    ?time dct:start ?startTime .
    ?time dct:end ?endTime .
    OPTIONAL { ?analyte skos:prefLabel ?albl . }
    OPTIONAL { ?location skos:prefLabel ?llbl . }
    OPTIONAL { ?dataset skos:prefLabel ?dlbl . }
}
    EOS

    log :info, "querying observations"
    res = sparql(query)
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

  # determine concepts from the given dimensions (all URIs), optionally limited
  # to those co-occurring with the given set of concepts from other dimensions
  # returns a hash of concepts by type - concepts are URI/labels pairs, with
  # labels indexed by language
  def determine_concepts(dimensions, concepts_by_dimension={},
      include_observations_count=false, include_hierarchy=false) # TODO: refactor, improve API
    make_query = lambda do |variables, conditions|
      query = <<-EOS.strip
PREFIX dct:<http://purl.org/dc/terms/>
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX qb:<http://purl.org/linked-data/cube#>

SELECT #{variables} WHERE {
#{conditions}
}
      EOS
      log :info, "querying concepts"
      return sparql(query, include_hierarchy)
    end
    unionize = lambda do |arr|
      return arr.length == 1 ? arr[0] : "\n{\n#{arr.join("\n} UNION {\n")}\n}\n"
    end

    pre_existing_conditions = concepts_by_dimension.each_with_index.
        map do |(dim, concepts), i|
      concepts = resource_list(concepts)
      [dimension_query(dim, i), "FILTER(?concept#{i} IN (#{concepts}))"].
          join("\n    ")
    end

    conditions = dimensions.map do |dim|
      [dimension_query(dim), "BIND (<#{dim}> AS ?type)",
          "OPTIONAL { ?concept skos:prefLabel ?label }"].join("\n    ")
    end
    conditions = unionize.call(conditions)
    conditions = ([conditions] + pre_existing_conditions).join("\n")

    query_variables = ["?type", "?concept", "?label"]
    query_conditions = conditions.clone
    if include_hierarchy
      query_variables += ["?grancestor", "?ancestor", "?parent"]
      query_conditions += "\n" + <<-EOS.rstrip
    OPTIONAL {
        ?parent skos:narrower ?concept .
        ?ancestor skos:narrowerTransitive ?concept .
        OPTIONAL { ?grancestor skos:narrower ?ancestor }
    }
      EOS
    end
    query_variables = "DISTINCT #{query_variables.join(" ")}"
    res = make_query.call(query_variables, query_conditions)
    concepts_by_type = res["results"]["bindings"].inject({}) do |memo, result| # TODO: error handling
      type = result["type"]["value"]
      concept = result["concept"]["value"]
      memo[type] ||= {}
      memo[type][concept] = {}
      if label = result["label"]
        lang = label["xml:lang"]
        memo[type][concept][lang] = label["value"]
      end
      if include_hierarchy
        memo["_hierarchy"] ||= []
        memo["_hierarchy"] << ["grancestor", "ancestor", "parent", "concept"].
            map { |key| result[key]["value"] rescue nil }
      end
      memo
    end
    hierarchy = concepts_by_type.delete("_hierarchy")

    if include_observations_count
      res = make_query.call("(COUNT(DISTINCT ?obs) AS ?obsCount)", conditions) # XXX: separate query inefficient
      obs_count = Float(res["results"]["bindings"][0]["obsCount"]["value"]).to_i
      ret = [concepts_by_type, obs_count]
    else
      ret = [concepts_by_type]
    end
    ret << self.class.resolve_hierarchy(hierarchy) if include_hierarchy
    return ret.length == 1 ? ret[0] : ret
  end

  def determine_dimensions
    query = <<-EOS.strip
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX qb:<http://purl.org/linked-data/cube#>

SELECT DISTINCT ?dim ?label WHERE {
    ?dim a qb:DimensionProperty .
    OPTIONAL {
        ?dim skos:prefLabel ?label
    }
}
    EOS
    log :info, "querying dimensions"
    return determine_labeled_resources(query, "dim")
  end

  def observations_count(concepts_by_dimension={})
    conditions = concepts_by_dimension.each_with_index. # XXX: largely duplicates `determine_concepts`
        map do |(dim, concepts), i|
      concepts = resource_list(concepts)
      [dimension_query(dim, i), "FILTER(?concept#{i} IN (#{concepts}))"].
          join("\n    ")
    end.join("\n")

    query = <<-EOS.strip
PREFIX dct:<http://purl.org/dc/terms/>
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX qb:<http://purl.org/linked-data/cube#>

SELECT (COUNT(DISTINCT ?obs) AS ?obsCount) WHERE {
#{conditions}
    ?obs a qb:Observation .
}
    EOS

    log :info, "querying observations count"
    res = sparql(query)
    return Float(res["results"]["bindings"][0]["obsCount"]["value"]).to_i
  end

  # returns a hash of URI/labels pairs, with labels indexed by language
  def determine_labeled_resources(query, binding) # TODO: rename
    res = sparql(query)
    return res["results"]["bindings"].inject({}) do |memo, result| # TODO: error handling
      id = result[binding]["value"]
      memo[id] ||= {}
      if label = result["label"]
        lang = label["xml:lang"]
        memo[id][lang] = label["value"]
      end
      memo
    end
  end

  # `var` is used as suffix to create pseudo-local variables
  def dimension_query(dim, var=nil) # TODO: rename
    if dim == "http://data.uba.de/led/temporal" # XXX: special-casing
      return <<-EOS.rstrip
      ?obs <#{dim}> ?time#{var} .
      ?time#{var} dct:start ?concept#{var} .
      ?obs a qb:Observation .
      EOS
    end

    return <<-EOS.rstrip
    ?obs <#{dim}> ?concept#{var} .
    ?obs a qb:Observation .
    EOS
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
