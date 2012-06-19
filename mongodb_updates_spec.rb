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
    
    specify 'Update one matching record. Use $set operator to set a field\'s value' do
      @collection.update({'category' => 'sandwich'}, {'$set' => {:meal => "lunch"}})

      count = @collection.find({'meal' => 'lunch'}).count
      count.should == 1
    end

    specify 'Update all matching records. Multi argument specifies that all documents should be updated' do
      @collection.update({'category' => 'sandwich'}, {'$set' => {:meal => "lunch"}}, {:multi => true })

      count = @collection.find({'meal' => 'breakfast'}).count
      count.should == 0
    end
    
    specify 'Increment : Change all' do
      @collection.update({:category => "sandwich"}, {'$inc' => {:num => 1 }}, {:multi => true})
      result = @collection.find({:num => 1 }).count
      
      result.should == 2
    end
    
    specify 'Increment : Increments a particular value by a certain amount' do
      @collection.update({:type => "peanut butter"}, {'$inc' => {:num => 1 }})
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
  
  specify 'Set all records for a given field with a value' do
    @collection = @db["foods"]
    @collection.insert({:category => "sandwich", :type => "peanut butter"})
    @collection.insert({:type => "tuna salad"})
    
    @collection.update({:type => "tuna salad"}, {'$set' => {:category => "sandwich"}}, {:multi => true })
    
    result = @collection.find({:category => "sandwich"}).count
    
    result.should == 2
  end

  specify 'Unset all records for a given field with a value. Unset operator deletes a given field.' do
    @collection = @db["foods"]
    @collection.insert({:category => "sandwich", :type => "peanut butter"})
    
    @collection.update({:category => "sandwich"}, {'$unset' => {:category => 1}}, {:multi => true })
    
    result = @collection.find({:category => "sandwich"}).count
    
    result.should == 0    
  end

  context 'Data set two' do
    before do
      @pix = @db['photos']
      @pix.insert({:file => 'pic1.jpg', :loc => 'beach', :camera => 'Nikon'})
      @pix.insert({:file => 'pic2.jpg', :loc => 'beach', :camera => 'Nikon'})
      @pix.insert({:file => 'pic3.jpg', :loc => 'city', :camera => 'Nikon'})
    end
    
    specify 'Pull removes each occurrence of a specified value from an array' do
      @pix.update({:file => "pic1.jpg"}, {'$set' => {:tags => ['people','food','music','food','art'] }})  
      @pix.update({:file => "pic1.jpg"}, {'$pull' => {:tags => "food"}})
      
      result = @pix.find({:file => "pic1.jpg"}).first['tags']
      result.should == ['people', 'music', 'art']
    end
    
    specify 'Pull All removes multiple elements with different values from an array' do
      @pix.update({:file => "pic1.jpg"}, {'$set' => {:tags => ['people', 'food', 'music', 'food', 'art'] }})  
      @pix.update({:file => "pic1.jpg"}, {'$pullAll' => {:tags => ['food','art'] }})
      
      result = @pix.find({:file => "pic1.jpg"}).first['tags']
      result.should == ['people', 'music']
    end
    
    specify 'Push : Appends a value to an array' do
      @pix.update({:file => "pic1.jpg"}, {'$push' => {:tags => "people" }})
      
      result = @pix.find({:file => "pic1.jpg"}).first['tags']
      
      result.should == ['people']
    end
      
    specify 'Push All : Appends several values to an array' do
      @pix.update({:file => "pic2.jpg"}, {'$pushAll' => {:tags => ['food', 'people'] }})
      @pix.update({:file => "pic2.jpg"}, {'$pushAll' => {:tags => ['music', 'art'] }})
      
      result = @pix.find({:file => "pic2.jpg"}).first['tags']
      
      result.should == ['food', 'people', 'music', 'art']
    end
    
    specify 'Push All : Push array to non-array field gives db error' do
      @pix.update({:file => "pic3.jpg"}, {'$pushAll' => {:loc => ['food', 'people'] }})
      
      @db.should be_error
    end
    
    specify 'Adding data to an array with $addToSet' do
      pending 'Find syntax to use addToSet'
      # @pix.insert({:file => 'pic10.jpg', :people => []})
        
    end
    
    specify 'Pop an element at the end' do
      @pix.update({:file => "pic1.jpg"}, {'$set' => {:tags => ['people','food','music','art'] }})  
      @pix.update({:file => "pic1.jpg"}, {'$pop' => {:tags => 1 }})
      
      result = @pix.find({:file => "pic1.jpg"}).first['tags']
      
      result.should == ['people','food','music']
    end

    specify 'Pop an element in the beginning' do
      @pix.update({:file => "pic1.jpg"}, {'$set' => {:tags => ['people','food','music','art'] }})  
      @pix.update({:file => "pic1.jpg"}, {'$pop' => {:tags => -1 }})
      
      result = @pix.find({:file => "pic1.jpg"}).first['tags']
      
      result.should == ['food','music','art']
    end
    
    specify 'Check status for update permitted case' do
      @pix.update({:file => "pic1.jpg"}, {'$push' => {:camera => "Canon"}})  
      
      @db.get_last_error['updatedExisting'].should be_nil
    end
    
    specify 'Check status for a failed update' do
      @pix.update({:file => "pic1.jpg"}, {'$push' => {:tags => "people"}})
      
      @db.get_last_error['updatedExisting'].should be_true
    end
  end
end