package com.neosemantix.ds.paxos;

/**
 * @author umeshpatil
 *
 */

import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

/**
 * @author umeshpatil
 *
 */
public class PaxosMain {
	
	
	private static List<ActorRef> participants;
	
	
	private static Object gc;
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		gc = new Object();
		Config cfg = Config.getInstance();
		final ActorSystem system = ActorSystem.create("Paxos");
		participants = new ArrayList<ActorRef>();
		for (int i=0; i< cfg.numParticipants; i++) {
			participants.add(system.actorOf(Participant.props(cfg.numParticipants, "Participant_" + i)));
		}
	}
	
	public static Object getGlocalCommon() {
		return gc;
	}
	
	public static List<ActorRef> getParticipants() {
		return participants;
	}
	
	public static void consensusReached() {
		System.exit(0);
	}

}
