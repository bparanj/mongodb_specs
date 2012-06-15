require 'mongo'

describe 'Mongodb Query Conditions' do
  before do
    connection = Mongo::Connection.new
    @db = connection.db('query_condtions')  
  end
  
  after do
    @db.collections.each do |collection|
      @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
    end  
  end
  
  context 'Examples using Numbers' do
    before do
      @numbers = @db["nums"]
      (1..100).each {|i| @numbers.insert(:num => i, :string => i.to_s)}
    end
    
    specify 'Select comparison using greater than' do
      result = @numbers.find({'num' => {'$gt' => 90}}).count
      
      result.should == 10
    end
    
    specify 'Select comparison using less than' do
      result = @numbers.find({'num' => {'$lt' => 21}}).count
      
      result.should == 20
    end
    
    specify 'Select comparison using greater than or equal' do
      result = @numbers.find({'num' => {'$gte' => 90}}).count
      
      result.should == 11
    end
    
    specify 'Select comparison using greater than' do
      result = @numbers.find({'num' => {'$lte' => 21}}).count
      
      result.should == 21
    end
    
    specify 'Select using ranges' do
      result = @numbers.find({'num' => {'$gt' => 90, '$lt' => 95}}).count
      
      result.should == 4
      
      result = @numbers.find({'num' => {'$gte' => 90, '$lte' => 95}}).count
      result.should == 6
    end
    
    specify 'Select using equal' do
      result = @numbers.find({'num' => 90}).count
      
      result.should == 1
    end

    specify 'Select using not equal' do
      result = @numbers.find({'num' => {'$ne' => 90}}).count
      
      result.should == 99
    end
    
    specify 'Select using not in' do
      result = @numbers.find({'num' => {'$not' => {'$in' => [90]}}}).count
      
      result.should == 99
    end

    specify 'Select using not not equal' do
      result = @numbers.find({'num' => {'$not' => {'$ne' => 90}}}).count
      
      result.should == 1
    end
    
    specify 'Select using in' do
      result = @numbers.find({'num' => {'$in' => [1,3,5]}}).count
      
      result.should == 3
    end
    
    specify 'Select using not in using nin' do
      result = @numbers.find({'num' => {'$nin' => [1,3,5]}}).count
      
      result.should == 97
    end
    
    specify 'Select using exists : Field exists' do
      result = @numbers.find({'num' => {'$exists' => true}}).count
      
      result.should == 100
    end
    
    specify 'Select using exists : Field does not exist' do
      result = @numbers.find({'foo' => {'$exists' => true}}).count
      
      result.should == 0
    end
    
    specify 'Select using regexp : Match a given field that begins with x' do
      result = @numbers.find({'string' => /^x/}).count

      result.should == 0
    end
    
    specify 'Select using regexp : Match a given field that ends with y' do
      result = @numbers.find({'string' => /y$/}).count

      result.should == 0
    end
    
  end
  
  context 'Examples using arrays' do
    before do
      @arrays = @db["arrays"]
      @arrays.insert({:value => [1,3]})
      @arrays.insert({:value => [1,3,5,7,9]})
      @arrays.insert({:value => [2,4,6,8]})
    end
    
    specify 'Select all : Result found' do
      result = @arrays.find({'value' => {'$all' => [1,3,5]}}).count
      
      result.should == 1
    end
    
    specify 'Select all : No result found' do
      result = @arrays.find({'value' => {'$all' => [1,3,5,11]}}).count
      
      result.should == 0
    end
    
    specify 'Select using size option : Result found' do
      result = @arrays.find({'value' => {'$size' => 4}}).count
      
      result.should == 1
    end

    specify 'Select using size option : No result found' do
      result = @arrays.find({'value' => {'$size' => 8}}).count
      
      result.should == 0
    end
    
    specify 'Select arrays which contains a given element' do
      result = @arrays.find({'value' => 3}).count
      
      result.should == 2
    end
  end
  
  context 'Examples for using other options' do
    specify 'Select using elemMatch option' do
      collection = @db["mercury"]
      collection.insert({:name => "post1", :ratings => [{:val => "super", :count => 1}, {:val => "boring", :count => 12 }] })
      
      result = collection.find('ratings' => {'$elemMatch' => {'val' => 'boring', 'count' => {'$gt' => 10}}}).count
      result.should == 1
    end
    
    specify 'Using dot notation arrays' do
      collection = @db['std']
      collection.insert("x" => [{'a' => 1, 'b' => 3}, 7, {'b' => 99}, {'a' => 11}])
      collection.insert("x" => [ { 'a' => 0, 'b' => 3 }, 8])
      
      result = collection.find({'x.a' => 11, 'x.b' => { '$gt' => 1 }}).count
      
      result.should == 1
      
      no_result = collection.find({'x.a' => 0, 'x.b' => {'$gt' => 10}}).count
      no_result.should == 0
    end
  end
  
end
