// Copyright 2019; All rights reserved with NeoSemantix, Inc.
package com.neosemantix.problems;

import java.util.ArrayList;
import java.util.List;

/**
 * Problem / objective:
 * We have one 2D array, filled with zeros and ones. We have to find the 
 * starting point and ending point of all rectangles filled with 0. 
 *  A rectangle might contain only one element.
 *  
 * Assumptions:
 * a. It is given that rectangles are separated and do not touch each other 
 * however they can touch the boundary of the array.
 * b. No overlapping of edges of 0 rectangles.
 * c. Input is valid, so no zig-zag zeros, uneven zero rows.
 * d. Input is not necessarily square i.e. same row & column count.
 *
 * 
 * Implementation:
 * Iterate through each row for value 1 stop growing any earlier rectangle
 * while for 0, either grow relevant rectangle or start new rectangle.
 * 
 * @author umeshpatil
 *
 */
public class ZeroRectangles {
	
	/**
	 * For each cell, we track to which 0 rectangle that cell belongs to;
	 * if any. For cell with 1, clearly it will not be associated with any
	 * rectangle of zeros.
	 */
	private Rectangle[][] associatedRectangles;
	
	/**
	 * List of 'references' to the same rectangles. Created so as when we
	 * print the output we do not have to scan the input rectangle but rather
	 * simply walk through rectangle list. It can give count directly too.
	 */
	private List<Rectangle> rectanglesCreated;
	
	/**
	 * Input row count.
	 */
	private int rowCount;
	
	/**
	 * Inout col count.
	 */
	private int colCount;
	
	/**
	 * Input array.
	 */
	private int[][] input;
	

	/**
	 * Constructor which process the input and prints output.
	 * 
	 * @param in
	 */
	public ZeroRectangles(int[][] in) {
		rowCount = in.length;
		colCount = in[0].length;
		input = in;
		associatedRectangles = new Rectangle[rowCount][colCount];
		rectanglesCreated = new ArrayList<Rectangle>();
		
		for(int row=0; row < rowCount; row++) {
			// walk through row by row
			processRow(row);
		}
		
		// Handle the last one, it would not grow any further.
		Rectangle lastOne = associatedRectangles[rowCount-1][colCount-1];
		if (lastOne != null) {
			lastOne.canHeightGrow = false;
			lastOne.canWidthGrow = false;
		}
		
		// print the output
		for(Rectangle r: rectanglesCreated) {
			System.out.println(r);
		}
	}
	
	/**
	 * Main for unit testing.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		int[][] input = {
		            {1, 1, 1, 1, 1, 1, 1},
		            {1, 1, 1, 1, 1, 1, 1},
		            {1, 1, 1, 0, 0, 0, 1},
		            {1, 0, 1, 0, 0, 0, 1},
		            {1, 0, 1, 1, 1, 1, 1},
		            {1, 0, 1, 0, 0, 0, 0},
		            {1, 1, 1, 0, 0, 0, 0},
		            {1, 1, 1, 1, 1, 1, 1}
		};
		ZeroRectangles zr = new ZeroRectangles(input);
	}
	
	/**
	 * Process the passed row, refers to class level state.
	 * 
	 * @param whichRow
	 */
	private void processRow(int whichRow) {
		Rectangle aboveRect = null;
		Rectangle adjacentRect = null;
		
		// for a given row, walk through all columns
		for(int col=0; col < colCount; col++) {
			
			// if not first row, find the above rectangle if associated
			if (whichRow > 0) {
				aboveRect = associatedRectangles[whichRow-1][col];
			}
			// if not first column, find the adjacent rectangle if associated
			if (col > 0) {
				adjacentRect = associatedRectangles[whichRow][col-1];
			}
			
			// get the cell value
			int val = input[whichRow][col];
			
			// if the cell value is 1
			if (val == 1) {
				
				// and there is above rectangle, it cannot grow anymore
				if (aboveRect != null) {
					aboveRect.canHeightGrow = false;
				}
				// and there is an adjacent rectangle, it cannot grow too
				if (adjacentRect != null) {
					adjacentRect.canWidthGrow = false;
				}
				// no rectangle can be associated with this cell
				associatedRectangles[whichRow][col] = null;
				
			} else {
				
				// it must be zero value
				if (aboveRect != null) {
					if (aboveRect.startI + aboveRect.height == whichRow) {
						// this is the first column....
						aboveRect.height++;
					}
					// associate the above rectangle
					associatedRectangles[whichRow][col] = aboveRect;
				}
				
				// it is the first row of the rectangle, keep growing
				if (adjacentRect != null && adjacentRect.startI == whichRow) {
						adjacentRect.width++;
						associatedRectangles[whichRow][col] = adjacentRect;
				}
				
				// time to create a new rectangle
				if (aboveRect == null && adjacentRect == null) {
					Rectangle newRec = new Rectangle(whichRow, col);
					associatedRectangles[whichRow][col] = newRec;
					rectanglesCreated.add(newRec);
				}
			}
		}
	}
	
	private class Rectangle {
		
		// starting topmost leftmost cell of the rectangle
		private int startI;
		private int startJ;
		
		// currently tracked width and height
		private int width;
		private int height;
		
		// these boolean flags may be redundant, but more useful for debugging
		private boolean canHeightGrow;
		private boolean canWidthGrow;
		
		Rectangle(int i, int j){
			startI = i;
			startJ = j;
			width = 1;
			height = 1;
			canHeightGrow = true;
			canWidthGrow = true;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("[("+startI+", "+startJ+") w: "+width+" h: "+height+" cHG: "+canHeightGrow+" cWG: "+canWidthGrow+"]");
			return sb.toString();
		}
	}

}
