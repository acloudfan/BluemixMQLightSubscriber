package com.acloudfan.mqsubscriber;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.mqlight.api.CompletionListener;
import com.ibm.mqlight.api.Delivery;
import com.ibm.mqlight.api.DestinationAdapter;
import com.ibm.mqlight.api.NonBlockingClient;
import com.ibm.mqlight.api.NonBlockingClientAdapter;
import com.ibm.mqlight.api.StringDelivery;
import com.ibm.mqlight.api.SubscribeOptions;

/**
 * Hello world!
 *
 */
public class MQSubscriber {

	private static final String SUBSCRIBE_TOPIC = "javamqlight/quotes";

	private static final String SHARE_ID = "mqsubscriber";

	/** Simple logging */
	private final static Logger logger = Logger.getLogger(MQSubscriber.class.getName());

	/** Declare the non blocking MQ client **/
	private NonBlockingClient mqlightClient;

	// Constructor
	public MQSubscriber() {
		try{
			NonBlockingClientAdapter<Void> adaptor = createNonBlockingClientAdaptor();
			logger.info("RECVD Non blocking "+adaptor);
			mqlightClient = NonBlockingClient.create(null, adaptor, null);
		}catch(Throwable e){
			logger.info("ERRRRR");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new MQSubscriber();
		
		while(true){
			try{Thread.sleep(20000);}catch(Exception e){}
			logger.info("Subscriber is Alive");
		}
	}

	/** Listener for events generated on client **/
	private NonBlockingClientAdapter<Void> createNonBlockingClientAdaptor(){
		return new NonBlockingClientAdapter<Void>(){

			@Override
			public void onStarted(NonBlockingClient client, Void context) {
				
				logger.info("SENDER - onStarted RECEVIVED");
				super.onStarted(client, context);
				logger.info("SENDER - Client Started");
				
				// now subscribe
				SubscribeOptions opts = SubscribeOptions.builder().setShare(SHARE_ID).build();
				
				try{
				client.subscribe(SUBSCRIBE_TOPIC, opts, new DestinationAdapter<Void>() {
		            public void onMessage(NonBlockingClient client, Void context, Delivery delivery) {
		              logger.log(Level.INFO,"Received message of type: " + delivery.getType());
		              StringDelivery sd = (StringDelivery)delivery;
		              logger.log(Level.INFO,"Data: " + sd.getData());
		            }
		          }, new CompletionListener<Void>() {
		            @Override
		            public void onSuccess(NonBlockingClient c, Void ctx) {
		              logger.log(Level.INFO, "Subscribed!");
		            }
		            @Override
		            public void onError(NonBlockingClient c, Void ctx, Exception exception) {
		              logger.log(Level.SEVERE, "Exception while subscribing. ", exception);
		            }
		          }, null);
				}catch(Throwable e){
					logger.info("ERROR SUBSCRIBING "+e.getMessage());
					e.printStackTrace();
				}
			}
			
		};
	};
}
