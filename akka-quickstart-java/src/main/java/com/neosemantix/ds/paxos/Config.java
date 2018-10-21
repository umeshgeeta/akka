package com.neosemantix.ds.paxos;

/**
 * @author umeshpatil
 *
 */
public class Config {

	public int numParticipants = 5;
	
	/**
	 * When it is less than 0, say -1, a participant will issue only one Prepare Request. 
	 * When it is greater than 0, say 5000, a participant will issue the next Prepare 
	 * Request after 5 seconds. In other words, this value represents time a participant
	 * would wait in milliseconds before the next Prepare Request is to be issued.
	 */
	public long waitBeforeNextRequest = -1;

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
