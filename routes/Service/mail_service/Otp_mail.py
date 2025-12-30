from fastapi.responses import JSONResponse
from email.message import EmailMessage
import smtplib
import ssl

async def Otp_mail( email, otp ):
    smtp_server = "smtpout.secureserver.net"
    smtp_port = 465
    smtp_user = "compliance@pridecons.com"
    smtp_pass = "Pride@#0308"  # move to environment variable in production

    try:
        # Create email
        msg = EmailMessage()
        msg["From"] = "Pride Trading Consultancy Pvt. Ltd. <compliance@pridecons.com>"
        msg["To"] = email
        msg["Subject"] = "Your OTP Code for Verification - Pride Trading Consultancy Private Limited"
        msg.set_content(f"""
        Dear User,

        Your One-Time Password (OTP) is: {otp}

        Please use this OTP to complete your verification process. 
        Do not share this code with anyone.

        If you did not request this, please ignore this email.

        Best regards,  
        Pride Trading Consultancy Private Limited
        """)

        # Send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        return {"message": "OTP Sent!","email":email}

    except Exception as e:
        return JSONResponse(content={"message": "Failed to send OTP.", "error": str(e)}, status_code=500)
