require 'mongo'

describe 'Mongodb Updates' do
  before do
    connection = Mongo::Connection.new
    @db = connection.db('query_updates')  
  end
  
  after do
    @db.collections.each do |collection|
      @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
    end  
  end
  
  context 'Data set one' do
    before do
      @collection = @db["foods"]
      @collection.insert({:category => "sandwich", :type => "peanut butter"})
      @collection.insert({:category => "sandwich", :type => "tuna salad"})      
    end
    
    specify 'Update one matching record' do
      @collection.update({'category' => 'sandwich'}, {'$set' => {:meal => "lunch"}})

      count = @collection.find({'meal' => 'lunch'}).count
      count.should == 1
    end

    specify 'Update all matching records' do
      @collection.update({'category' => 'sandwich'}, {'$set' => {:meal => "lunch"}}, {:multi => true })

      count = @collection.find({'meal' => 'breakfast'}).count
      count.should == 0
    end
    
    specify 'Increment : Change all' do
      @collection.update({:category => "sandwich"}, {'$inc' => {:num => 1 }}, {:multi => true})
      result = @collection.find({:num => 1 }).count
      
      result.should == 2
    end
    
    specify 'Increment : Change one' do
      @collection.update({:type => "peanut butter"}, {'$inc' => {:num => 1 }}, {:multi => true})
      result = @collection.find({:num => 1 }).count
      
      result.should == 1
    end
    
  end

  specify 'Upsert : If results of selection (1st param) is empty then insert' do
    @collection = @db["foods"]
    @collection.insert({'_id' => 1, :category => "sandwich", :type => "peanut butter"})
    @collection.update({}, {'_id' => 1, :category => "sandwich", :type => "tuna fish"}, {:upsert => true})
    
    changed_record_count = @collection.find.count
    changed_record_count.should == 1
    
    result = @collection.find({'type' => 'tuna fish'}).count
    result.should == 1
    
    @collection.update({'_id' => 2}, {'_id' => 2, :category => "soup", :type => "potato"}, {:upsert => true, :multi => false })
    after_insert_count = @collection.find.count
    
    after_insert_count.should == 2
    
    @collection.update({:category => "salad"}, {'_id' => 3, :category => "salad", :type => "potato"}, {:upsert => true, :multi => false })
    count = @collection.find.count
    
    count.should == 3
  end
  


end