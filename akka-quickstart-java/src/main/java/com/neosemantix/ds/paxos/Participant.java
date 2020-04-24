package com.neosemantix.ds.paxos;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import com.neosemantix.ds.paxos.Protocol.PrepareResponseState;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Main class which encapsulates behavior of a participant in this consensus
 * algorithm. It models a participant's behavior for various messages received
 * from other participants. It is an Akka actor with a timer to create starting
 * request to start consensus building. In the constructor of this class a timer
 * is started which will create the first tick at a randomly chosen interval.
 * 
 * Internally it tracks state about requests send and what responses have
 * arrived for those requests as well as how it responded to requests of other
 * participants.
 * 
 * @author umeshpatil
 */
public class Participant extends AbstractActorWithTimers {
	
	// *************************************************************************
	// Static definitions and methods
	// *************************************************************************

	public static int NO_PROPOSAL_ACCEPTED_YET = -1;
	public static int PROPOSAL_VALUE_NOT_APPLICABLE = -1;
	
	private static Config cfg = Config.getInstance();
	
	private static Object TICK_KEY = "TickKey";

	private static final class FirstTick {
	}

	private static final class Tick {
	}

	static public Props props(int pCount, String name) {
		return Props.create(Participant.class, () -> new Participant(pCount, name));
	}
	
	private static Map<ActorRef, String> actorRefToNameMap = new HashMap<ActorRef, String>();
	
	// *************************************************************************
	// ProposalNumberGenerator
	// *************************************************************************

	/**
	 * Simple proposal generator, it gives next number to whatever highest proposL
	 * numbers this participant has encountered.
	 * 
	 * Each participant has it's own copy, meaning each participants proposals start
	 * from 1. Many proposals from different participants would have the same number.
	 */
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
	
	// *************************************************************************
	// CirculatedProposal
	// *************************************************************************

	/**
	 * State information about proposals circulated by this participant to others.
	 */
	private static class CirculatedProposal {

		private Participant proposer;
		private long birthdate;
		private int participantCount;
		private ProposalNumberGenerator propNumGenarator;
		private boolean acceptReqSent;
		private int proposalValue;
		private int proposalNumber;
		private int highestProposalNumAmongResponsesReceived;
		private int preparResponsesReceived;
		private int acceptResponsesReceived;
		private int prepareRequestsIssuedSoFar;
		private long whenLastPrepareRequestIssued;
		
		private CirculatedProposal(ProposalNumberGenerator propNumGen, int pc, Participant p, long bd) {
			propNumGenarator = propNumGen;
			participantCount = pc;
			proposer = p;
			birthdate = bd;
		}

		private int circulateNewPrepareProposal(Participant issuer) {
			// push this new proposal numbers to all participants
			preparResponsesReceived = 0;
			highestProposalNumAmongResponsesReceived = 0;
			proposalValue = 0;
			proposalNumber = propNumGenarator.getNextProposalNumber();
			acceptReqSent = false; // reset
			Protocol.PrepareRequest prepReq = new Protocol.PrepareRequest(proposalNumber);
			whenLastPrepareRequestIssued = issuer.issueRequests(prepReq);
			prepareRequestsIssuedSoFar++;
			return proposalNumber;
		}

		private long lastPrepareRequest() {
			return whenLastPrepareRequestIssued;
		}

		private int prepReqIssuedSoFar() {
			return this.prepareRequestsIssuedSoFar;
		}

		private synchronized void trackPrepareResponse(Protocol.PrepareResponse pr) {
			if (pr.getState() != PrepareResponseState.REJECTED) {
				preparResponsesReceived++;
				int highestPropNo = pr.getLastPreparedRequestProposalNo();
				if (highestProposalNumAmongResponsesReceived < highestPropNo) {
					highestProposalNumAmongResponsesReceived = highestPropNo;
					if (pr.getState() == PrepareResponseState.ACCEPTED) {
						proposalValue = pr.getLastAcceptedProposalValue();
					}
				}
				if (majority(preparResponsesReceived)) {
					// we got majorities responding to the prepare proposal
					// circulate the accept proposal now
					if (!acceptReqSent) {
						// we have not issued accept request so far
						circulateAcceptProposal(proposer);
						acceptReqSent = true; // no more additional
					}
					// else already accept request is in circulation...
					// What happens is we get responses from majority participants for prepare
					// request and this this participant starts issuing the accept request. 
					// However, responses for the earlier prepare request still keeps on 
					// coming from participants beyond the majority. At that point we do not 
					// want to issue additional acceptance request since one is already in 
					// circulation for this prepare request. Point is we do not for the accept
					// request to get responses to prepare request from all participants. 
					// But beyond what prepare responses are received, need not cause
					// additional accept request.
				}
				// else will have to wait for more responses
			}
		}

		/**
		 * For 0 and 1 participant count, it is always majority. These are just
		 * pathological corner cases. For participant count 2, majority is attained any
		 * time a participant hears from the other.
		 * 
		 * For odd count of participants, say 3, 3 / 2 = 1 (integer division); getting
		 * response from one single participant is enough since the 'self' is already
		 * counted. For 5, 5 / 2 = 2; receiving from 2 other plus 'self' is the
		 * majority.
		 * 
		 * For even count, say 4, 4 / 2 = 2; getting responses from 2 other participants
		 * is enough because 'self' takes it over the half count.
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
			if (proposalValue == 0) {
				proposalValue = 5;
			} else {
				proposalValue += 5;
			}
			Protocol.AcceptRequest acptReq = new Protocol.AcceptRequest(proposalNumber,
					proposalValue);
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
				outToAllLoggers("Consensus reached in " + (System.currentTimeMillis() - birthdate)
						+ " milliseonds for proposal from " + proposer 
						+ " Proposal number: " + proposalNumber
						+ " Proposal value: " + proposalValue
						+ " highestProposalNumAmongResponsesReceived: " + highestProposalNumAmongResponsesReceived);
				outToAllLoggers(line);
				PaxosMain.consensusReached();
			}
		}

		private void outToAllLoggers(String msg) {
			System.out.println(msg);
			proposer.msgLog.info(msg);
		}

	}
	
	// *************************************************************************
	// RespondedProposal
	// *************************************************************************

	/**
	 * State information about proposals responded by this participant.
	 */
	private static class RespondedProposal {

		// Proposal number of last prepared request responded
		private int propNumOfLastPrepReqResd;

		// accepted proposals
		private int acceptedProposalNumber;
		private int acceptedProposalValue;

		private RespondedProposal() {
			acceptedProposalNumber = NO_PROPOSAL_ACCEPTED_YET;
			acceptedProposalValue = PROPOSAL_VALUE_NOT_APPLICABLE;
			propNumOfLastPrepReqResd = NO_PROPOSAL_ACCEPTED_YET;
		}

	}
	
	// *************************************************************************
	// Class level variables
	// *************************************************************************

	private LoggingAdapter msgLog = Logging.getLogger(getContext().getSystem(), this);
	
	private String name;
	private CirculatedProposal propCirculated;
	private RespondedProposal propResponded;
	private Random random;
	
	// *************************************************************************
	// Constructor
	// *************************************************************************

	/**
	 * @param pc
	 *            Participant count
	 */
	public Participant(int pc, String n) {
		propResponded = new RespondedProposal();
		propCirculated = new CirculatedProposal(new ProposalNumberGenerator(propResponded), pc, this,
				System.currentTimeMillis());
		random = new Random();
		getTimers().startSingleTimer(TICK_KEY, new FirstTick(), Duration.ofMillis(((1 + random.nextInt(9)) * 100)));
		this.name = n;
		actorRefToNameMap.put(getSelf(), name);
		msgLog.debug("Created " + this);
	}

	@Override
	public void preStart() {
		msgLog.debug("Starting " + this);
	}

	@Override
	public void preRestart(Throwable reason, Optional<Object> message) {
		msgLog.error(reason, "Restarting due to [{}] when processing [{}]", reason.getMessage(),
				message.isPresent() ? message.get() : "");
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
		Protocol.PrepareResponse response = new Protocol.PrepareResponse();
		if (propResponded.propNumOfLastPrepReqResd < prepReq.proposalNumber) {
			if (propResponded.propNumOfLastPrepReqResd == NO_PROPOSAL_ACCEPTED_YET) {
				response.setState(PrepareResponseState.NONE_PROMISED_OR_ACCEPTED);
			} else if (propResponded.propNumOfLastPrepReqResd > NO_PROPOSAL_ACCEPTED_YET) {
				response.setState(PrepareResponseState.PROMISED_NONE_ACCEPTED);
			}
			propResponded.propNumOfLastPrepReqResd = prepReq.proposalNumber;
			response.setLastPreparedRequestProposalNo(prepReq.proposalNumber);
		}
		// else we ignore, already set to REJECTED

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

	/**
	 * @param req
	 * @return long Timestamp when issuing of requests to all participants is complete
	 */
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
	
	private String getName(ActorRef arf) {
		String name = actorRefToNameMap.get(arf);
		if (name == null) {
			msgLog.error("Could not fine name for: " + arf);
			name = arf.toString();
		}
		return name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see akka.actor.AbstractActor#createReceive()
	 */
	@Override
	public Receive createReceive() {
		return receiveBuilder().match(Protocol.PrepareRequest.class, prepReq -> {
			msgLog.debug("       Received " + prepReq + " by " + this + " from " + getName(getSender()));
			Protocol.PrepareResponse resp = respond(prepReq);
			if (resp != null) {
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.AcceptRequest.class, accpReq -> {
			msgLog.debug("       Received " + accpReq + " by " + this + " from " + getName(getSender()));
			Protocol.AcceptResponse resp = respond(accpReq, getSender());
			if (resp != null) {
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.PrepareResponse.class, prepResp -> {
			msgLog.debug(">->->- Received " + prepResp + " by " + this + " from " + getName(getSender()));
			propCirculated.trackPrepareResponse(prepResp);
		}).match(Protocol.AcceptResponse.class, accpResp -> {
			msgLog.debug(">>->>- Received " + accpResp + " by " + this + " from " + getName(getSender()));
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
				// consensus should have been established by now. So try new proposal afresh.
				msgLog.debug(
						"Circulating new prepare request (" + propCirculated.prepReqIssuedSoFar() + ") by " + this);
				// do something useful here - hold on for now from initiating multiple proposals
				// by the same participant
				propCirculated.circulateNewPrepareProposal(this);
			}
			// else we need to give time for current requests to make progress
		}).build();
	}

}
