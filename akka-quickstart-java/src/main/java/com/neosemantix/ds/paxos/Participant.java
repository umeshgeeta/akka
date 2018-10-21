/**
 * 
 */
package com.neosemantix.ds.paxos;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;

//import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * @author umeshpatil
 *
 */
public class Participant extends AbstractActorWithTimers {

	private static int NO_PROPOSAL_ACCEPTED_YET = -1;
	private static int PROPOSAL_VALUE_NOT_APPLICABLE = -1;

	private LoggingAdapter msgLog = Logging.getLogger(getContext().getSystem(), this);
	
	private static Config cfg = Config.getInstance();

	static public Props props(int pCount, String name) {
		return Props.create(Participant.class, () -> new Participant(pCount, name));
	}

	private static class ProposalNumberGenerator {

		private RespondedProposal respondedProposals;

		private ProposalNumberGenerator(RespondedProposal respProp) {
			respondedProposals = respProp;
		}

		private int getNextProposalNumber() {
			int pn = Math.max(respondedProposals.propNumOfLastPrepReqResd, respondedProposals.acceptedProposalNumber);
			return pn + 1;
		}

	}

	private static class CirculatedProposal {

		private int participantCount;
		private int issuedPrepareRequestNumber;
		private ProposalNumberGenerator propNumGenarator;
		private int preparResponsesReceived;
		private int acceptResponsesReceived;
		private int highestProposalNoOfResponsesReceived;
		private int valueOfHighestProposalReceived;
		private Participant parent;
		private long birthdate;
		private long lastPrepareRequest;
		private int prepReqIssuedSoFar;
		private boolean acceptReqAlreadySent;		

		private CirculatedProposal(ProposalNumberGenerator propNumGen, int pc, Participant p, long bd) {
			propNumGenarator = propNumGen;
			participantCount = pc;
			parent = p;
			birthdate = bd;
		}

		private int circulateNewPrepareProposal(Participant issuer) {
			// push this new proposal numbers to all participants
			preparResponsesReceived = 0;
			highestProposalNoOfResponsesReceived = 0;
			valueOfHighestProposalReceived = 0;
			issuedPrepareRequestNumber = propNumGenarator.getNextProposalNumber();
			acceptReqAlreadySent = false;	//reset
			Protocol.PrepareRequest prepReq = new Protocol.PrepareRequest(issuedPrepareRequestNumber);
			lastPrepareRequest = issuer.issueRequests(prepReq);
			prepReqIssuedSoFar++;
			return issuedPrepareRequestNumber;
		}
		
		private long lastPrepareRequest() {
			return lastPrepareRequest;
		}
		
		private int prepReqIssuedSoFar() {
			return this.prepReqIssuedSoFar;
		}

		private synchronized void trackPrepareResponse(int highestPropNo, int propVal) {
			preparResponsesReceived++;
			if (highestProposalNoOfResponsesReceived < highestPropNo) {
				highestProposalNoOfResponsesReceived = highestPropNo;
				valueOfHighestProposalReceived = propVal;
			}
			if (majority(preparResponsesReceived)) {
				// we got majorities responding to the prepare proposal
				// circulate the accept proposal now
				if (!acceptReqAlreadySent) {
					// we have not issued accept request so far
					circulateAcceptProposal(parent);
					acceptReqAlreadySent = true;	// no more additional
				}
				// else already accept request is in circulation...
				// What happens is we get responses from majority participants for prepare request 
				// and this this participant starts issuing the accept request. However, responses
				// for the earlier prepare request still keeps on coming from participants beyond
				// the majority. At that point we do not want to issue additional acceptance request
				// since one is already in circulation for this prepare request. Point is we do
				// not for the accept request to get responses to prepare request from all 
				// participants. But beyond what prepare responses are received, need not cause
				// additional accept request.
			}
			// else will have to wait for more responses
		}

		/**
		 * For 0 and 1 participant count, it is always majority. These are just pathological corner cases.
		 * For participant count 2, majority is attained any time a participant hears from the other.
		 * 
		 * For odd count of participants, say 3, 3 / 2 = 1 (integer division); getting response from one
		 * single participant is enough since the 'self' is already counted. For 5, 5 / 2 = 2; receiving 
		 * from 2 other plus 'self' is the majority.
		 * 
		 * For even count, say 4, 4 / 2 = 2; getting responses from 2 other participants is enough 
		 * because 'self' takes it over the half count.
		 * 
		 * @param arg
		 * @return boolean
		 */
		private boolean majority(int arg) {
			boolean result = false;
			switch (participantCount) {
			case 0:
			case 1:
				result = true;
				break;
			case 2:
				if (arg == 1)
					result = true;
				break;
			default:
				if (arg >= (participantCount / 2)) {
					result = true;
				}
			}
			return result;
		}

		private void circulateAcceptProposal(Participant issuer) {
			if (valueOfHighestProposalReceived == 0) {
				valueOfHighestProposalReceived  = 5;
			} else {
				valueOfHighestProposalReceived += 5;
			}
			Protocol.AcceptRequest acptReq = new Protocol.AcceptRequest(issuedPrepareRequestNumber,
					valueOfHighestProposalReceived);
			acceptResponsesReceived = 0;
			// push it to all participants
			issuer.issueRequests(acptReq);
		}

		private synchronized void trackAcceptResponse() {
			acceptResponsesReceived++;
			// when acceptResponsesReceived == number of participants; consensus is reached
			// participantCount - 1 because we skip message to self
			if (acceptResponsesReceived == (participantCount - 1)) {				
				String line = "==========================================================";
				outToAllLoggers(line);
				outToAllLoggers("Consensus reached in "+ (System.currentTimeMillis() - birthdate)  + " milliseonds for proposal from " + parent + "  highestProposalNoOfResponsesReceived: "  + 
							highestProposalNoOfResponsesReceived + " valueOfHighestProposalReceived: " + valueOfHighestProposalReceived);
				outToAllLoggers(line);
				PaxosMain.consensusReached();
			}
		}
		
		private void outToAllLoggers(String msg) {
			System.out.println(msg);
			parent.msgLog.info(msg);
		}

	}

	private static class RespondedProposal {

		// Proposal number of last prepared request responded
		private int propNumOfLastPrepReqResd;

		// accepted proposals
		private int acceptedProposalNumber;
		private int acceptedProposalValue;
		// private ActorRef proposer;

		private RespondedProposal() {
			acceptedProposalNumber = NO_PROPOSAL_ACCEPTED_YET;
			acceptedProposalValue = PROPOSAL_VALUE_NOT_APPLICABLE;
			propNumOfLastPrepReqResd = NO_PROPOSAL_ACCEPTED_YET;
		}

	}

	private String name;
	private CirculatedProposal propCirculated;
	private RespondedProposal propResponded;
	private Random random;

	private static Object TICK_KEY = "TickKey";

	private static final class FirstTick {
	}

	private static final class Tick {
	}

	/**
	 * @param pc
	 *            Participant count
	 */
	public Participant(int pc, String n) {
		propResponded = new RespondedProposal();
		propCirculated = new CirculatedProposal(new ProposalNumberGenerator(propResponded), pc, this, System.currentTimeMillis());
		random = new Random();
		getTimers().startSingleTimer(TICK_KEY, new FirstTick(), Duration.ofMillis(((1+random.nextInt(9)) * 100)));
		this.name = n;
		msgLog.debug("Created " +  this + " with ActorRef as " + this.getSelf());
	}
	
	  @Override
	  public void preStart() {
	    msgLog.debug("Starting");
	  }

	  @Override
	  public void preRestart(Throwable reason, Optional<Object> message) {
	    msgLog.error(reason, "Restarting due to [{}] when processing [{}]",
	      reason.getMessage(), message.isPresent() ? message.get() : "");
	  }
	
	public String toString() {
		return name;
	}
	
	private long prepReqResponseDelay() {
		int i = random.nextInt(11);
		if (i == 0) {
			i++;
		}
		return i * 100;
	}

	private Protocol.PrepareResponse respond(Protocol.PrepareRequest prepReq) {
		Protocol.PrepareResponse response = null;
		if (propResponded.propNumOfLastPrepReqResd < prepReq.proposalNumber) {
			response = new Protocol.PrepareResponse(propResponded.acceptedProposalNumber,
					propResponded.acceptedProposalValue);
			propResponded.propNumOfLastPrepReqResd = prepReq.proposalNumber;
		}
		// else we ignore
		
		// artificial delay
		Object globalC = PaxosMain.getGlocalCommon();
		synchronized (globalC) {
			try {
				globalC.wait(prepReqResponseDelay());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return response;
	}

	private Protocol.AcceptResponse respond(Protocol.AcceptRequest accpReq, ActorRef proposer) {
		Protocol.AcceptResponse resp = null;
		if (accpReq.proposalNumber >= propResponded.propNumOfLastPrepReqResd) {
			resp = new Protocol.AcceptResponse();
			propResponded.acceptedProposalNumber = accpReq.proposalNumber;
			propResponded.acceptedProposalValue = accpReq.proposalValue;
			// propResponded.proposer = proposer;
		}
		// else it has responded to another Prepare Request which has number
		// higher than this Accept Request proposal number.
		return resp;
	}

	private synchronized long issueRequests(Protocol.Request req) {
		List<ActorRef> participants = PaxosMain.getParticipants();
		if (participants != null && !participants.isEmpty()) {
			for (ActorRef p : participants) {
				if (p.compareTo(getSelf()) != 0) {
					p.tell(req, getSelf());
				}
				// else skip messages to self
			}
		}
		return System.currentTimeMillis();
	}

	@Override
	public Receive createReceive() {
		ActorRef ar = getSender();
		return receiveBuilder().match(Protocol.PrepareRequest.class, prepReq -> {
			msgLog.debug("       Received "+ prepReq + " by " + this + " from " + getSender());
			Protocol.PrepareResponse resp = respond(prepReq);
			if (resp != null) {
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.AcceptRequest.class, accpReq -> {
			msgLog.debug("       Received " + accpReq + " by " + this + " from " + getSender());
			Protocol.AcceptResponse resp = respond(accpReq, getSender());
			if (resp != null) {
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.PrepareResponse.class, prepResp -> {
			msgLog.debug(">->->- Received " + prepResp + " by " + this + " from " + getSender());
			propCirculated.trackPrepareResponse(prepResp.lastAcceptedProposal, prepResp.lastAcceptedProposalValue);
		}).match(Protocol.AcceptResponse.class, accpResp -> {
			msgLog.debug(">>->>- Received " + accpResp + " by "  + this + " from " + getSender());
			propCirculated.trackAcceptResponse();
		}).match(FirstTick.class, message -> {
			msgLog.debug("Circulating prepare request by " + this);
			// do something useful here
			propCirculated.circulateNewPrepareProposal(this);
			getTimers().startPeriodicTimer(TICK_KEY, new Tick(), Duration.ofSeconds(1));
		}).match(Tick.class, message -> {
			long lastReq = propCirculated.lastPrepareRequest();
			long howMuchToWait = cfg.waitBeforeNextRequest; 
			if (howMuchToWait > 0 && (lastReq < System.currentTimeMillis() - howMuchToWait)) {
				// Last prepare request by this Participant was way back, 
				// consensus should have been established by now. So trt=y new proposal afresh.
				msgLog.debug("Circulating new prepare request (" + propCirculated.prepReqIssuedSoFar() + ") by " + this);
				// do something useful here - hold on for now from initiating multiple proposals by the same participant
				propCirculated.circulateNewPrepareProposal(this);
			}
			// else we need to give time for current requests to make progress
		}).build();
	}

}
