// Copyright 2019; All rights reserved with NeoSemantix, Inc.
package com.neosemantix.problems.hackerrank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Objective:	For a given string, in main, find out how many substring pairs 
 * 				are there which are anagrams. For a substring of 1 character
 * 				it means at how many places that character is repeated in the
 * 				string. Same substring at 2 different places, including overlap,
 * 				will be considered as a valid paid. That is why the answer for
 * 				"kkkk" is it would have 10 substring pairs of anagrams.
 * 
 * Theory:		Anagrams CANNOT be built recursively. For example strings
 * 				"wxyz" and "xywz" is an anagram pair but we do not have any
 * 				anagram pair of substrings of length 3. 
 * 
 * Implementation: Essentially that means we cannot use 'memoization' type of 
 * 				technique. Every substring pair, we sort and check if it is an
 * 				anagram pair. Part of the problem is we generate substrings in
 * 				enumerated manner.
 * 
 * 				Here we maintain a structure of anagram pairs and substrings 
 * 				which are left out; all organized by lengths of substrings.
 * 
 * 				The subclass Substring plays a pivotal role, it defines the
 * 				substring on the original string and has critical equals method
 * 				which comes into the picture at the time adding to the list.
 * 
 * Problem source: Hackerrank, medium difficulty, 50 point worth.
 * 
 * 				https://www.hackerrank.com/challenges/sherlock-and-anagrams/problem
 * 
 * @author umeshpatil
 *
 */
public class CountAnagrams {
	
	private String input;
	private int inputLength;
	private int maxSubstringLength;
	private Map<Integer, List<List<Substring>>> anagramSet;
	private Map<Integer, List<Substring>> leftOutSet;

	/**
	 * Constructor which retains the string for which anagram substring
	 * pairs are found.
	 * 
	 * @param in
	 */
	public CountAnagrams(String in) {
		input = in;
		System.out.println("Input: " + input);
		inputLength = input.length();
		anagramSet = new HashMap<Integer, List<List<Substring>>>();
		leftOutSet = new HashMap<Integer, List<Substring>>();
		List<Substring> sSet = generateNextSubstring();
		while (sSet != null) {
			System.out.println("\nsSet: " +sSet + "\n");
			addAnagram(sSet);
			printAnagramsFoundSoFar();
			sSet = generateNextSubstring();
		}
	}
	
	/**
	 * Debug method to print anagrams found so far.
	 */
	private void printAnagramsFoundSoFar() {
		StringBuffer sb = new StringBuffer();
		Set<Integer> lengths = anagramSet.keySet();
		sb.append("\n");
		for (Integer l: lengths) {
			sb.append("{"+l+": --> ");
			List<List<Substring>> ls = anagramSet.get(l);
			sb.append(" [" + ls + "] ");
			sb.append("}");
		}
		sb.append("\n");
		System.out.println(sb.toString());
	}
	
	/**
	 * Final call to count all anagram pairs found so far.
	 * 
	 * @return int
	 */
	private int countAnagramSubstrings() {
		int result = 0;
		Set<Integer> subSizes = anagramSet.keySet();
		for (Integer i: subSizes) {
			List<List<Substring>> lists = anagramSet.get(i);
			for(List<Substring> aList: lists) {
				result += anagramPairCount(aList.size());
			}
		}
		return result;
	}
	
	/**
	 * Testing and validition purposes.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// INPUT ---> OUTPUT
		// "kkkk" --> 10
		// "abcd" --> 0
		// "abba" --> 4
		CountAnagrams ca = new CountAnagrams("kkkk");	
		System.out.print("Count: " + ca.countAnagramSubstrings());
	}
	
	/**
	 * @param sSet
	 * @param i
	 * @param aSubstr
	 * @return Whether aSubstr is anagram with ith Substring in sSet.
	 */
	private boolean checkAnagram(List<Substring> sSet, int i, Substring aSubstr) {
		return sSet.get(i).isAnagram(aSubstr);
	}
	
	/**
	 * @param sSet Sort given set of Substrings into those which are anagrams
	 * 					and those which are left out.
	 */
	private void addAnagram(List<Substring> sSet) {
		while (!sSet.isEmpty()) {
			Substring aSubstr = sSet.remove(0);
			List<Substring> anaSet = new ArrayList<Substring>();
			anaSet.add(aSubstr);
			int i=0;
			while (i < sSet.size()) {
				if (checkAnagram(sSet, i, aSubstr)){
					Substring sr = sSet.remove(i);
					if (!anaSet.contains(sr)) {
						anaSet.add(sr);
					}
					// already present
				} else {
					i++;
				}
			}
			if (anaSet.size() == 1) {
				// did not find any anagram
				List<Substring> ls = leftOutSet.get(maxSubstringLength);
				if (ls != null && ls.size() > 0) {
					Substring ss = anaSet.get(0);
					if (!ls.contains(ss)) {
						ls.add(ss);
					}
				} else {
					// we are adding left out for the first time
					leftOutSet.put(maxSubstringLength, anaSet);
				}
			} else {
				// at the minimum, anagram pair was found
				List<List<Substring>> anaSets = anagramSet.get(maxSubstringLength);
				if (anaSets == null) {
					anaSets = new ArrayList<List<Substring>>();
					anaSets.add(anaSet);
					anagramSet.put(maxSubstringLength, anaSets);
					System.out.println("\n Added: " + maxSubstringLength + " anaSets: " + anaSets);
				} else {
					anaSets.add(anaSet);
				}
			}
		}
	}
	
	/**
	 * @param substringLength
	 * @return For the given length of substring of the original string,
	 * 			create possible substring of given length.
	 */
	private Set<Substring> getSubstringSet(int substringLength){
		Set<Substring> result = new HashSet<Substring>();
		List<List<Substring>> anagramSets = anagramSet.get(substringLength);
        if (anagramSets != null && anagramSets.size() > 0) {
            for(List<Substring> as: anagramSets) {
                result.addAll(as);
            }
        }
		List<Substring> ls = leftOutSet.get(substringLength);
		if (ls != null) {
			result.addAll(ls);
		}
		return result;
	}
	
	/**
	 * @return Creates the list of substrings of next length and returns.
	 */
	private List<Substring> generateNextSubstring() {
		List<Substring> set = null;
		if (leftOutSet.isEmpty() && anagramSet.isEmpty()) {
			// first time, we do single char strings
			set = new ArrayList<Substring>();
			for (int i=0; i<inputLength; i++) {
				set.add(new Substring(i, i+1));
			}
			maxSubstringLength = 1;
		} else if (maxSubstringLength < inputLength - 1){
			Set<Substring> lastSubstringSet = getSubstringSet(maxSubstringLength);
			set = new ArrayList<Substring>();
			for(Substring ss: lastSubstringSet) {
				Substring sl = ss.generateLeft();
				if (sl != null && !set.contains(sl)) {
					set.add(sl);
				}
				Substring sr = ss.generateRight();
				if (sr != null && !set.contains(sr)) {
					set.add(sr);
				}
			}
			maxSubstringLength++;
		} 
		// else we done with generating substrings
		return set;
	}
	
	/**
	 * @param anagramSubstringSetSize
	 * @return Count of anagram pairs for the given set size.
	 * 			It is Sum(i) for i over 0 to n-1.
	 */
	private int anagramPairCount(int anagramSubstringSetSize) {
		int sum = 0;
		for (int i=0; i<anagramSubstringSetSize; i++) {
			sum += i;
		}
		return sum;
	}
	
	
	/**
	 * Substring for the given input string.
	 *
	 */
	private class Substring {
		private int start;	// inclusive
		private int end;	// exclusive
		private int length;
		
		/**
		 * @param s
		 * @param e
		 */
		private Substring(int s, int e) {
			start = s;
			end = e;
			length = end - start;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			return "[" + start + ", " + end + ", " + get() + "]";
		}
		
		/**
		 * @param other
		 * @return True if the method argument is anagram to this Substring.
		 */
		private boolean isAnagram(Substring other) {
			boolean result = false;
			String myStr = get();
			String oth = other.get();
			if (myStr.length() == oth.length()) {
				char[] myStrC = myStr.toCharArray();
				char[] othC = oth.toCharArray();
				Arrays.sort(myStrC);
				Arrays.sort(othC);
				result = (new String(myStrC)).equals(new String(othC));
			}
			// else result remains false
			return result;
		}
		
		/**
		 * @return Generate a Substring where current substring is stretched 
		 * 			on left by one character if possible; else null.
		 */
		private Substring generateLeft() {
			if (start > 0) {
				return new Substring(start-1, end);
			} else {
				// no more space on Left to add a char
				return null;
			}
		}
		
		/**
		 * @return Generate a Substring where current substring is stretched 
		 * 			on right by one character if possible; else null.
		 */
		private Substring generateRight() {
			if (end < inputLength) {
				return new Substring(start, end+1);
			} else {
				// no more space on Right to add a char
				return null;
			}
		}
		
		/**
		 * @return The underlying substring for the given input.
		 */
		private String get() {
			return input.substring(start, end);
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		public boolean equals(Object o) {
			boolean result = false;
			if (o != null) {
				if (o == this) {
					result = true;
				} else if (o instanceof Substring) {
					Substring os = (Substring)o;
					if (start == os.start && end == os.end) {
						return true;
					} 
				}
			}
			return result;
		}
	}
}
