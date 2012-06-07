require 'mongo'

describe 'Mongodb Embedded Documents' do
  before do
    connection = Mongo::Connection.new
    @db = connection.db('matrix')
    @embedded = @db["embedded"]
    @embedded.insert({:order_num => 101, :customer => 'Acme', :ship_via => 'UPS',
        :shipto => {:street => "123 E Chicago Ave",
                    :city => "Chicago", :state => "IL", :zip => 60611},
        :lines => [ {:line_num => 1, :item => 'widget', :quantity => 2},
	                  {:line_num => 2, :item => 'gizmo', :quantity => 4},
		                {:line_num => 3, :item => 'thingy', :quantity => 6}  ]})
    @embedded.insert({:order_num => 102, :customer => 'Ace',  :ship_via => 'UPS',
        :shipto => {:street => "123 N Wabash St",
                    :city => "Chicago", :state => "IL", :zip => 60611},    
        :lines => [ {:line_num => 1, :item => 'widget', :quantity => 10},
	                  {:line_num => 2, :item => 'gizmo', :quantity => 1}   ]})	
    @embedded.insert({:order_num => 103, :customer => 'Acme', :ship_via => 'FedEx',
        :shipto => {:street => "123 E Wacker Dr",
                    :city => "Chicago", :state => "IL", :zip => 60611},    
        :lines => [ {:line_num => 1, :item => 'thingy', :quantity => 5}  ]})	
  end
  
  after do
    @db.collections.each do |collection|
      @db.drop_collection(collection.name) unless collection.name =~ /indexes$/
    end
  end
  
  specify 'Query by given number and Count' do
    number_of_orders = @embedded.find({'order_num' => 103}).count
    
    number_of_orders.should == 1
  end

  specify 'Query by accessing collection with a given number and Count' do
    number_of_orders = @embedded.find({'lines.line_num' => 3}).count
    number_of_orders.should == 1
    
    docs_count = @embedded.find({'lines.line_num' => 2}).count    
    docs_count.should == 2
  end

  specify 'Query using where like equivalent and Count' do
    number_of_orders = @embedded.find({'shipto.zip' => 60611}).count
    
    number_of_orders.should == 3
  end
  
  specify 'Accessing an existing collection within an embedded document' do
    line_list = @embedded.find({:order_num => 103}).first['lines']
    line_list.count.should == 1
  end

  specify 'Add to an existing collection within an embedded document' do
    line_list = @embedded.find({:order_num => 103}).first['lines']
    @embedded.update({:order_num => 103}, 
                     {'$set' => {:lines => (line_list << {:line_item => 2, :item => "gizmo", :quantity => 10}) }})
    result = @embedded.find({:order_num => 103 }).first['lines'].count
    
    result.should == 2
  end
  
  specify 'Update an array containing a record as a hash' do
    current_quantity = @embedded.find({:order_num => 103}).first['lines'][0]['quantity']
    current_quantity.should == 5
    
    @embedded.update(({:order_num => 103}), {'$set' => {'lines.0.quantity' => 1}})
    updated_quantity = @embedded.find({:order_num => 103}).first['lines'][0]['quantity']
    updated_quantity.should == 1
    
    @embedded.update(({:order_num => 103}), {'$inc' => {'lines.0.quantity' => 1}})
    incremented_quantity = @embedded.find({:order_num => 103}).first['lines'][0]['quantity']
    incremented_quantity.should == 2
  end
  
  specify 'Update a field in existing record' do
    current_zip = @embedded.find({:order_num => 103 }).first['shipto']['zip']
    current_zip.should == 60611
    
    @embedded.update(({:order_num => 103}), {'$set' => {'shipto.zip' => 60606}})
    updated_zip = @embedded.find({:order_num => 103 }).first['shipto']['zip']
    updated_zip.should == 60606
  end
  
  specify 'Creating an index and doing an indexed query' do
    @embedded.create_index('lines.item')
    widget_count = @embedded.find({'lines.item' => 'widget'}).count
    widget_count.should == 2
    
    result = @embedded.find({'lines.item' => 'widget'}).explain['indexBounds']['lines.item'][0][0]
    result.should == 'widget'
  end
  
end