package com.solace.openshift.demo.xmlConverter;

import java.util.concurrent.Semaphore;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.statistics.StatType;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XML_to_JSON_Converter_JCSMP_Native implements XMLMessageListener{

	String SOL_USER = null;
	String SOL_PASSWORD = null;
	String SOL_TOPIC = null;
	String SOL_QUEUE = null;
	String SOL_HOST = null;
	String SOL_TOPIC_RESEND = null;
	String SOL_VPN = null;
	JCSMPSession session = null;
	XMLMessageConsumer cons = null;
	XMLMessageProducer prod = null;
	Semaphore sem = null;
	Topic topic;
	Topic topicResend;
	private static boolean stopping = false;
	final int PRETTY_PRINT_INDENT_FACTOR = 4;
	final Logger logger = LoggerFactory.getLogger(XML_to_JSON_Converter_JCSMP_Native.class);

	public static void main(String[] args) {
		XML_to_JSON_Converter_JCSMP_Native instance = new XML_to_JSON_Converter_JCSMP_Native();
		instance.run(args);

	}

	public void run(String[] args) {
		// Get required Solace details from System Variables
		SOL_USER = System.getenv("SOL_USER");
		SOL_PASSWORD = System.getenv("SOL_PASSWORD");
		SOL_TOPIC = System.getenv("SOL_TOPIC");
		SOL_QUEUE = System.getenv("SOL_QUEUE");
		SOL_HOST = System.getenv("SOL_HOST");
		SOL_TOPIC_RESEND = System.getenv("SOL_TOPIC_RESEND");
		SOL_VPN = System.getenv("SOL_VPN");

		sem = new Semaphore(0);

		final Logger logger = LoggerFactory.getLogger(XML_to_JSON_Converter_JCSMP_Native.class);


		logger.info("Createing JSCMP Session");
		JCSMPProperties properties = new JCSMPProperties();

		properties.setProperty(JCSMPProperties.HOST, SOL_HOST);
		properties.setProperty(JCSMPProperties.USERNAME, SOL_USER);
		properties.setProperty(JCSMPProperties.PASSWORD, SOL_PASSWORD);
		properties.setBooleanProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);
		properties.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false);
		properties.setProperty(JCSMPProperties.VPN_NAME, SOL_VPN);

		if( System.getenv("SOL_TOPIC_RESEND") !=null) {
			topicResend = JCSMPFactory.onlyInstance().createTopic(System.getenv("SOL_TOPIC_RESEND"));
			logger.info("Publishing Topic is set to: " + topicResend);
		} else {
			topicResend = JCSMPFactory.onlyInstance().createTopic("bank/data/json");
			logger.info("Publish Topic is set to: " + topicResend);
		}
		// Channel properties
		JCSMPChannelProperties cp = (JCSMPChannelProperties) properties
				.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
		cp.setConnectRetries(-1);
		cp.setConnectRetriesPerHost(0);
		cp.setConnectTimeoutInMillis(2000);
		cp.setReconnectRetryWaitInMillis(2000);
		cp.setReconnectRetries(-1);

		try {
			session =  JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();
		} catch (InvalidPropertiesException e) {
			logger.info("Failed to create session because of invalid JCSMPproperties");
			e.printStackTrace();
			finish(1);
		} catch (JCSMPException e) {
			logger.info("Failed to create Session");
			e.printStackTrace();
			finish(1);
		}

		try {
			prod = session.getMessageProducer(new PubCallback());
		} catch (JCSMPException e) {
			//possible there was a DR fail-over
			logger.info(e.getLocalizedMessage());
			e.printStackTrace();
		}

		if( System.getenv("SOL_TOPIC") !=null) {
			topic = JCSMPFactory.onlyInstance().createTopic(SOL_TOPIC);
			logger.info("Subscribing Topic from systemenv is set to: " + topic);
		} else {
			topic = JCSMPFactory.onlyInstance().createTopic("bank/data/xml");
			logger.info("Subscribing Topic is set to: " + topic);
		}

		try {
			cons = session.getMessageConsumer(this);
			session.addSubscription(topic);
			cons.start();
		} catch (JCSMPException e) {
			logger.info("Error creating Topic Consumer or adding subscription:\n" + e.getLocalizedMessage());
			e.printStackTrace();
		}


		ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
		flowProps.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
		flowProps.setNoLocal(true);
		flowProps.setTransportWindowSize(120);
		Endpoint queue = JCSMPFactory.onlyInstance().createQueue("restQ");
		flowProps.setEndpoint(queue);
		FlowReceiver flow;
		try {
			flow = session.createFlow(this, flowProps);
			flow.start();
			logger.info("Listening on Queue: " + queue.getName());
		} catch (JCSMPException e) {
			logger.info("Error connecting to Queue " + SOL_QUEUE + ": \n" +e.getLocalizedMessage());
			e.printStackTrace();
		}

		blockClientFromEnding();

		/*
       try {
		Thread.sleep(1000000000);
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		 */
	}



	public boolean convertAndSend(String msgText, BytesXMLMessage _msg ) {
		boolean status = true;
		TextMessage textMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		JSONObject jsonObject = null;
		String jsonText = null;
		try {
			jsonObject = XML.toJSONObject(msgText);
			jsonText = jsonObject.toString(PRETTY_PRINT_INDENT_FACTOR);

		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		//System.out.println(msgText);
		textMsg.clearAttachment();
		textMsg.clearContent();
		textMsg.setText(jsonText);
		textMsg.setDeliveryMode(DeliveryMode.PERSISTENT);
		textMsg.setDeliverToOne(true);




		// Set the message's correlation key. This reference
		// is used when calling back to responseReceivedEx().
		if (_msg.getCorrelationId() != null) {
			textMsg.setCorrelationId(_msg.getCorrelationId());
		}


		try {
			prod.send(textMsg, topicResend);
		} catch (JCSMPException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return status;

	}

	public void onException(JCSMPException je) {
		logger.info(" +++++++++++++ Received JCSMPException:\n\t" + je.getLocalizedMessage());

	}

	public void onReceive(BytesXMLMessage msg) {
		if (msg instanceof TextMessage) {
			convertAndSend(((TextMessage) msg).getText(), msg);
		} else {
			//System.out.println("=======================not text");
			logger.info("Received non-Text messages");
		}




	}

	/**
	 * Close the session and exit.
	 */
	protected void finish(final int status) {
		if (session != null) {
			printSessionStats(session);
			session.closeSession();
		}
		System.exit(status);
	}

	/**
	 * Prints the most relevant session stats
	 * @param s
	 *            parameter
	 */
	static void printSessionStats(JCSMPSession s) {
		if (s == null) return;
		System.out.println("Number of messages sent: "
				+ s.getSessionStats().getStat(StatType.TOTAL_MSGS_SENT));
		System.out.println("Number of messages received: "
				+ s.getSessionStats().getStat(StatType.TOTAL_MSGS_RECVED));
	}

	//Makes sure asynchronous call backs continue to be available by prevent application from ending
	void blockClientFromEnding()
	{
		while(!stopping)
		{
			//block waiting for semaphore
			try
			{
				logger.info("+++++++++++ About to aquire semaphore");
				sem.acquire();
				logger.info("+++++++++++ No longer blocking on semaphore semaphore");

			}
			catch(InterruptedException e)
			{
				//System.err.println("Failed to create Solace Connection");
				logger.error("Failed to create Solace Connection");
				logger.warn("Caught Interrupted Exception on Semaphore");
				stopping = true;
				sem.release();
			}

		}

		System.out.println("Everything is down...");
		System.out.flush();
	}

	void stopSolace()
	{
		logger.info("Stopping consumer and session");
		if(session != null)
		{
			System.out.println("FinalStats:\n" + session.getSessionStats().toString());
			System.out.flush();
			cons.close();
			session.closeSession();
			System.out.println("Stopping consumer and session...");
			System.out.flush();
			logger.info("Shuttting Down!");
			stopping = true;
			sem.release();
		}
	}

	//Class that is called when JVM is interrupted. The thread is called
	//to run when the JVM is interrupted.
	class SolaceShutdownHook extends Thread
	{
		//JCSMPSession h_session = null;
		//XMLMessageConsumer h_cons = null;

		public void run()
		{
			logger.info("\nCaught shutdown interrupt....");
			stopSolace();

		}

	}

}
