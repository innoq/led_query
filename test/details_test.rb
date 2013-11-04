require File.expand_path("../test_helper.rb", __FILE__)

class DetailsTest < DefaultTest

  def test_concept_details
    rdf = File.read(@common) + <<-EOS
led:upb a qb:DataSet, skos:Concept;
    skos:inScheme led:sourceScheme;
    skos:prefLabel "Umweltprobenbank"@de .

led:lead a skos:Concept;
    skos:inScheme led:analyteScheme;
    skos:prefLabel "Blei"@de;
    skos:prefLabel "lead"@en .
    EOS
    @store.add_triples @repo, "text/turtle", rdf

    rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    skos = "http://www.w3.org/2004/02/skos/core#"
    qb = "http://purl.org/linked-data/cube#"

    info = @db.resource_details("#{@led}upb").map do |prd, obj|
      "#{prd}: #{obj.to_a.join(" | ")}"
    end
    assert_equal info.sort.join("\n"), <<-EOS.strip
#{rdf}type: <#{skos}Concept> | <#{qb}DataSet>
#{skos}inScheme: <#{@led}sourceScheme>
#{skos}prefLabel: "Umweltprobenbank"@de<>
    EOS

    info = @db.resource_details("#{@led}lead").map do |prd, obj|
      "#{prd}: #{obj.to_a.join(" | ")}"
    end
    assert_equal info.sort.join("\n"), <<-EOS.strip
#{rdf}type: <#{skos}Concept>
#{skos}inScheme: <#{@led}analyteScheme>
#{skos}prefLabel: "lead"@en<> | "Blei"@de<>
    EOS
  end

end
