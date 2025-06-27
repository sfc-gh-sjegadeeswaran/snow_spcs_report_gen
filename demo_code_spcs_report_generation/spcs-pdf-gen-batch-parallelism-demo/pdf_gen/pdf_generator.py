""" Python Module to generate PDF reports based on the input dataframe values"""

from fpdf import FPDF
import pandas as pd

class pdfgen:
    def __init__(self, title: str):
        self.title = title
        self.pdf = FPDF(format='letter')
        self.pdf.add_page()
        self.pdf.set_font("Arial", size=10)
        self.pdf.cell(0, 10, title, ln=True, align="C")
        self.pdf.ln(10) # Add a line break
    
    def generate_pdf(self, data: pd.DataFrame):
        with self.pdf.table() as table:
            header = data.columns.tolist()
            row = table.row()
            for column in header:
                row.cell(column)            
            for index, data_row in data.iterrows():
                row = table.row()
                for item in data_row:
                    row.cell(str(item))
        return None
    
    def save_pdf(self, filename: str):
        """Generates the PDF and saves it to a file."""
        self.pdf.output(filename)
  