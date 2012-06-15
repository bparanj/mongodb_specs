require 'mongo'

describe 'Mongodb Query Options' do
  before do
    connection = Mongo::Connection.new
    @db = connection.db('query_options')  
    @collection = @db["num_strings"]
    (1..100).each {|i| @collection.insert(:item => i, :item_string => i.to_s)}
  end
  
  after do
    @db.collections.each do |collection|
      @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
    end  
  end
  
  specify 'Select one field' do
    fields_size = @collection.find({'item' => 33}, {:fields => "item"}).first.size
    
    fields_size.should == 2
  end

  specify 'Select all fields' do
    fields_size = @collection.find({'item' => 33}).first.size
    
    fields_size.should == 3
  end
  
  specify 'Limit returned docs' do
    result_array = @collection.find({}, {:limit => 2}).to_a
    
    result_array.size.should == 2
  end
  
  specify 'Count ignores limit' do
    result_array = @collection.find({}, {:limit => 2}).count
    
    result_array.size.should == 8
  end
  
  specify 'Skip returned docs' do
    result = @collection.find({}, {:skip => 1}).first['item_string']
    
    result.should == "2"
  end
  
  specify 'Sort returned docs : string value' do
    result = @collection.find.to_a[99]['item_string']
    
    result.should == "100"
    
    descended_result = @collection.find({}, {:sort => ['item_string', :desc] }).first['item_string']
    descended_result.should == '99'
  end
  
  specify 'Sort returned docs : integer value' do
    last_item = @collection.find.to_a[99]['item']
    
    last_item.should == 100
    
    integer_result = @collection.find({}, {:sort => ['item', :desc]}).first['item']
    integer_result.should == 100
  end
end
