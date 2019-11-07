package com.neosemantix.ds.paxos;

/**
 * This class defines configuration items like number of participants to be
 * considered in a single consensus simulation. It also has another parameter
 * about whether participants should make multiple Prepare Requests and if so
 * then at what interval. In general, various inputs parameters to run a 
 * particular simulation exercise, or a set of simulations; should be defined
 * in this class.
 * 
 * The class implements Singleton design pattern.
 * 
 * @author umeshpatil
 */
public class Config {

	public int numParticipants = 25;
	
	/**
	 * When it is less than 0, say -1, a participant will issue only one Prepare Request. 
	 * When it is greater than 0, say 5000, a participant will issue the next Prepare 
	 * Request after 5 seconds. In other words, this value represents time a participant
	 * would wait in milliseconds before the next Prepare Request is to be issued.
	 */
	public long waitBeforeNextRequest = 500;

	private Config() {
	}
	
	private static Config cfg;

	public static Config getInstance() {
		if (cfg == null) {
			synchronized (PaxosMain.class) {
				if (cfg == null) {
					cfg = new Config();
				}
			}
		}
		return cfg;
	}

}
