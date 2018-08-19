/**
 * 
 */
package com.neosemantix.ds.paxos;

import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

/**
 * @author umeshpatil
 *
 */
public class PaxosMain {
	
	private static int numParticipants = 7;
	private static List<ActorRef> participants;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final ActorSystem system = ActorSystem.create("Paxos");
		participants = new ArrayList<ActorRef>();
		for (int i=0; i<numParticipants; i++) {
			participants.add(system.actorOf(Participant.props(numParticipants), "Participant_" + i));
		}
	}
	
	public static List<ActorRef> getParticipants() {
		return participants;
	}
	
	public static void consensusReached() {
		System.exit(0);
	}

}
