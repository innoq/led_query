require File.expand_path("../test_helper.rb", __FILE__)

class MetadataTest < DefaultTest

  def test_observations
    prefixes = ["@prefix dcat: <http://www.w3.org/ns/dcat#>.",
        "@prefix dct: <http://purl.org/dc/terms/>."].join("\n")
    rdf = prefixes + File.read(@common) + <<-EOS.strip
led:soil123 a dcat:Dataset, qb:Observation;
    qb:dataSet led:bodenportal;
    led:location led:westerzgebirge;
    led:observedMedia led:groundwater;
    led:analyte led:sand;
    led:temporal [ dct:start 1996; dct:end 1996 ].

led:bodenportal a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "Bodenportal"@de;
    dcat:landingPage <http://bodendaten.de>.

led:westerzgebirge a skos:Concept;
    skos:inScheme led:locationScheme;
    skos:prefLabel "Westerzgebirge"@de.

led:groundwater a skos:Concept;
    skos:inScheme led:observedMediaScheme;
    skos:prefLabel "Grundwasser"@de.

led:sand a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Sand"@de.
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    selected_concepts = { "#{@led}location" => ["#{@led}westerzgebirge"] }
    count = @DB.observations_count(selected_concepts)
    observations = @DB.determine_observations(selected_concepts)
    assert_equal count, 1
    assert_equal observations.length, count
    results = observations.map do |obs|
      ["obs", "mean", "uom", "time", "analyte", "location", "source"].
          map { |key| obs[key].to_s }.join(" | ")
    end.sort.join("\n")
    assert_equal results, <<-EOS.strip
#{@led}soil123 |  |  | [1996, 1996] | "Sand"<#{@led}sand> | "Westerzgebirge"<#{@led}westerzgebirge> | "Bodenportal"<#{@led}bodenportal>
    EOS
  end

end
