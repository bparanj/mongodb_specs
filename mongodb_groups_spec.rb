require 'mongo'

describe 'Mongodb Groups' do
  before do
    connection = Mongo::Connection.new
    @db = connection.db('my_groups')  
  end
  
  after do
    @db.collections.each do |collection|
      @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
    end  
  end
  
  context 'Size and Count' do
    before do
      @numbers = @db['nums']
      (1..100).each {|i| @numbers.insert(:num => i, :string => i.to_s )}
    end
    
    specify 'Counting records' do
      result = @numbers.find({}).count
      result.should == 100
    end

    specify 'Using greater than to filter records' do
      result = @numbers.find({'num' => {'$gt' => 50}}).count
      result.should == 50
    end    
  end
  
  context 'Distinct' do
    before do
      @addresses = @db['addresses']
      @addresses.insert({'city' => 'chicago', 'zip' => 60606, 'tags' => ['metra', 'cta_bus'], 'use' => {'commercial' => 80, 'residential' => 20}})
      @addresses.insert({'city' => 'chicago', 'zip' => 60611, 'tags' => ['cta_rail', 'cta_bus'], 'use' => {'commercial' => 60, 'residential' => 40}})
    end
    
    specify 'Select distinct records' do    
      count_distinct_fields = @addresses.distinct('city').count
      count_distinct_fields.should == 1

      distinct_field_count = @addresses.distinct('zip').count
      distinct_field_count.should == 2

      distinct_fields = @addresses.distinct('zip')
      distinct_fields.should == [60606, 60611]

      count_distinct_selected_fields = @addresses.distinct('zip', {'tags' => ['metra', 'cta_bus']}).count
      count_distinct_selected_fields.should == 1
    end

    specify 'Nested distinct' do
      distinct_nested_fields_count = @addresses.distinct('use.commercial').count
      distinct_nested_fields_count.should == 2
      
      distinct_nested_fields = @addresses.distinct('use.commercial')
      distinct_nested_fields.should == [60, 80]
    end    
  end
  
  context 'Group and Aggregation' do
    before do
      @zips = @db['zips']
      @zips.insert({:city => 'chicago', :state => 'IL', :zip => 60606, :population => 1000})
      @zips.insert({:city => 'chicago', :state => 'IL', :zip => 60607, :population => 1100})
      @zips.insert({:city => 'chicago', :state => 'IL', :zip => 60608, :population => 1200})
      @zips.insert({:city => 'decatur', :state => 'IL', :zip => 62521, :population => 1001})
      @zips.insert({:city => 'decatur', :state => 'IL', :zip => 62522, :population => 1002})
      @zips.insert({:city => 'decatur', :state => 'IL', :zip => 62523, :population => 1003})      
    end  

    specify 'Simple group' do    
      group_by_one_field_result = @zips.group([:city], {}, {}, 'function() {}', true)
      group_by_one_field_result.should == [{'city' => "chicago"}, {'city' => "decatur"}]
    end

    specify 'Simple aggregation' do
      group_by_one_field_result = @zips.group([:city], {}, {'zsum' => 0}, 'function(doc,out) {out.zsum += doc.population;}', true)
      group_by_one_field_result.should == [{'city' => 'chicago', 'zsum' => 3300}, {'city' => 'decatur', 'zsum' => 3006}]
    end
    
    specify 'Group by one field, two aggregate fields' do
      result = @zips.group([:city], {}, {'zsum' => 0, 'zstr' => ''}, 'function(doc,out) { out.zsum += doc.population; out.zstr += doc.state;}', true)
      result.should == [{'city' => 'chicago', 'zsum' => 3300.0, 'zstr' => 'ILILIL'}, {'city' => 'decatur', 'zsum' => 3006, 'zstr' => 'ILILIL'}]
    end
    
    specify 'Aggregation with finalize' do
      result = @zips.group([:city], {}, {'zsum' => 0, 'zc' => 0, 'avg_pop' => 0},
                  'function(doc,out) { out.zsum += doc.population; out.zc += 1; }',
                  'function(out) { out.avg_pop = out.zsum / out.zc}')
      result.should == [{'city' => 'chicago', 'avg_pop' => 1100, 'zsum' => 3300.0, 'zc' => 3.0},
                        {'city' => 'decatur', 'avg_pop' => 1002, 'zsum' => 3006.0, 'zc' => 3.0}]
    end
    
    specify 'Aggregation with condition' do
      result = @zips.group([:city], {'city' => 'chicago'}, {'zsum' => 0 }, 
                                     'function(doc, out) { out.zsum += doc.population; }', true)
      result.should == [{'city' => 'chicago', 'zsum' => 3300.0}]
    end
  end
  
end