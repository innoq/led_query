require File.expand_path("../test_helper.rb", __FILE__)

class LabelsTest < DefaultTest

  def test_resource_labels
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

    labels_by_resource = @db.resource_labels("#{@led}lead", "#{@led}upb")
    res = labels_by_resource["#{@led}upb"].
        map { |lang, label| "#{lang}: #{label}" }.sort.join("\n")
    assert_equal res, <<-EOS.strip
de: Umweltprobenbank
    EOS
    res = labels_by_resource["#{@led}lead"].
        map { |lang, label| "#{lang}: #{label}" }.sort.join("\n")
    assert_equal res, <<-EOS.strip
de: Blei
en: lead
    EOS
  end

end
