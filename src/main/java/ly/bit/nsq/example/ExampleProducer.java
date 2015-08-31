package ly.bit.nsq.example;

import ly.bit.nsq.NSQProducer;

public class ExampleProducer
{
	
	public static void main( String... args )
	{
		NSQProducer producer = new NSQProducer( "http://101.200.188.159:4151", "testTopit" );
		
		for ( int i = 0; i < 100; i++ )
		{
			try
			{
				String message = "{\"foo\":\"bar_" + i + "\"}";
				System.out.println( message );
//				producer.put( message );
				producer.putAsync( message );
				Thread.sleep( 100 );
			}
			catch ( InterruptedException e )
			{
				e.printStackTrace();
			}
		}
		
		producer.shutdown();
	}
	
}
