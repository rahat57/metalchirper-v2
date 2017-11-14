package org.xululabs.javaservices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.Vertx;



public class MainJavaServices {
	
	private static Logger log = LogManager.getRootLogger();

	public static void main(String[] args) {
		log.info("Initializing metalchirper-v2");
		
		Vertx vertx = Vertx.vertx();
		
//		org.xululabs.metalchirper_v2.Main
//		org.xululabs.javaservices.MainJavaServices
		
		
//		vertx.deployVerticle(new SearchingServer());
		vertx.deployVerticle(new IndexingServer());

	}

}
