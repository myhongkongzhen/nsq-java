package ly.bit.nsq.example;

import ly.bit.nsq.NSQProducer;

public class ExampleProducer
{
	
	public static void main( String... args )
	{
		NSQProducer producer = new NSQProducer( "http://101.200.188.159:4151", "native_nsq_api_pub_test" );
		
		for ( int i = 0; i < 100; i++ )
		{
			String message = "{\"foo\":\"bar_" + i + "\"}";
			System.out.println( message );
			producer.putAsync( message );
		}
		
		producer.shutdown();
	}
	
}
