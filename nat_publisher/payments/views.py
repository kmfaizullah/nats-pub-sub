import json
from django.http import JsonResponse
from django.shortcuts import render
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from payments.services.nats import NATSClient


@method_decorator(csrf_exempt, name="dispatch")
class CreatePaymentView(View):

    async def post(self, request):
        data = json.loads(request.body)

        payment = {
            "payment_id": data["payment_id"],
            "amount": data["amount"],
            "currency": data["currency"],
        }

        client = NATSClient()

        try:
            await client.publish(
                "payment.created",
                payment,
            )

            return JsonResponse({
                "status": "published",
                "payment": payment,
            })

        finally:
            await client.close()