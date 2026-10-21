/**
 * Fixed lines the system says without the model (no cost per use), written per language ahead of time.
 * Missing translations fall back to English. `{name}` placeholders are filled by `say`.
 */

import { LocalizedText, type Language } from "../core/index.ts";
import { Template } from "../domains/index.ts";

export const STRINGS = {
    fallbackText: {
        en: "Sorry, I had trouble with that. Could you try again?",
        es: "Lo siento, tuve un problema con eso. ¿Podría intentarlo de nuevo?",
    },
    fallbackVoice: {
        en: "Sorry, I had a hiccup there - what were you asking about?",
        es: "Perdón, tuve un pequeño problema - ¿qué me preguntaba?",
    },
    goodbye: {
        en: "Thanks so much for reaching out to {business_name}!{hours_sentence} Take care!",
        es: "¡Muchas gracias por comunicarse con {business_name}!{hours_sentence} ¡Que le vaya bien!",
    },
    hoursSentence: {
        en: " We're open {hours}.",
        es: " Nuestro horario es {hours}.",
    },
    unavailableText: {
        en: "Thanks for reaching out to {business_name}! We can't reply here right now - please call us{phone_sentence}.",
        es: "¡Gracias por comunicarse con {business_name}! No podemos responder por aquí en este momento; por favor llámenos{phone_sentence}.",
    },
    awayText: {
        en: "Thanks for your message! {business_name} will get back to you as soon as we're available.",
        es: "¡Gracias por su mensaje! {business_name} le responderá en cuanto esté disponible.",
    },
    profileGate: {
        en: "Sure, I can help with that. Before I do, could I get your first name and 10-digit phone number? We use these so we can text you appointment confirmations and follow up if you have any questions.",
        es: "Claro, con gusto le ayudo. Antes, ¿me puede dar su nombre y su número de teléfono de 10 dígitos? Los usamos para enviarle confirmaciones de citas por mensaje de texto y darle seguimiento.",
    },
    optedOut: {
        en: "You've been unsubscribed from {business_name} messages. Reply START to resubscribe.",
        es: "Ha cancelado su suscripción a los mensajes de {business_name}. Responda START para volver a suscribirse.",
    },
    optedIn: {
        en: "You're resubscribed to {business_name} messages. Reply STOP to opt out anytime.",
        es: "Se ha vuelto a suscribir a los mensajes de {business_name}. Responda STOP para cancelar en cualquier momento.",
    },
    help: {
        en: "{business_name}: reply here with any question{phone_sentence}. Msg & data rates may apply. Reply STOP to opt out.",
        es: "{business_name}: responda aquí con cualquier pregunta{phone_sentence}. Pueden aplicarse tarifas de mensajes y datos. Responda STOP para cancelar.",
    },
    smsPrimer: {
        en: "Reply STOP to opt out.",
        es: "Responda STOP para cancelar.",
    },
    voicemailGreeting: {
        en: "Thanks for calling {business_name}. Nobody can take your call right now, so please leave your name, number and a short message after the tone, and we'll get back to you.",
        es: "Gracias por llamar a {business_name}. En este momento nadie puede atender su llamada; por favor deje su nombre, número y un breve mensaje después del tono, y le devolveremos la llamada.",
    },
    stillThere: {
        en: "Hey, you still there?",
        es: "¿Sigue ahí?",
    },
    reprompt: {
        en: "Sorry, I didn't quite catch that - mind saying it one more time?",
        es: "Perdón, no le entendí bien - ¿me lo puede repetir?",
    },
    silenceGoodbye: {
        en: "Alright, I'll let the team know you reached out. Take it easy!",
        es: "Bueno, le avisaré al equipo que llamó. ¡Que esté bien!",
    },
    unconfigured: {
        en: "Sorry, this number isn't set up yet. Goodbye.",
        es: "Lo siento, este número todavía no está configurado. Adiós.",
    },
    greetings: [
        {
            en: "{part_of_day}, thank you for calling {business_name}. This is {agent_name} - and who do I have the pleasure of speaking with?",
            es: "{part_of_day}, gracias por llamar a {business_name}. Le habla {agent_name}. ¿Con quién tengo el gusto?",
        },
        {
            en: "Thank you for calling {business_name}, this is {agent_name}. May I ask who I'm speaking with?",
            es: "Gracias por llamar a {business_name}, le habla {agent_name}. ¿Me podría decir con quién hablo?",
        },
        {
            en: "{part_of_day}, this is {agent_name} over at {business_name}. Who do I have the pleasure of speaking with today?",
            es: "{part_of_day}, le habla {agent_name} de {business_name}. ¿Con quién tengo el gusto?",
        },
    ],
    partOfDay: {
        morning:   { en: "Good morning", es: "Buenos días" },
        afternoon: { en: "Good afternoon", es: "Buenas tardes" },
        evening:   { en: "Good evening", es: "Buenas noches" },
    },
} as const satisfies Record<string, LocalizedText | readonly LocalizedText[] | Record<string, LocalizedText>>;

/**
 * A fixed line in `language`, with placeholders filled.
 */
export function say(text: LocalizedText, language: Language, values: Readonly<Record<string, string>> = {}): string {
    return Template.render(LocalizedText.in(text, language), values);
}
